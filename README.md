# ShopBrasil — Atividade 01: Pipeline de Métricas de Catálogo (Apache Airflow)

Substitui o script via cron que alimentava o painel matinal do time de
pricing. Fluxo: **FakeStore API → métricas por categoria → PostgreSQL**,
todo dia às 06:00 (horário de Brasília).

## Estrutura

```
lab-airflow-shopbrasil/
├── docker-compose.yml                      # Airflow + 2 bancos Postgres
├── dags/shopbrasil_metricas_catalogo.py    # DAG principal (TaskFlow API)
├── sql/init.sql                            # cria a tabela no banco analítico
└── logs/                                   # gerada automaticamente pelo Airflow
```

## Arquitetura do DAG

```
shopbrasil_metricas_catalogo
│
├── TaskGroup: ingestao                          (linear)
│   ├── buscar_produtos          → GET /products na FakeStore API
│   └── listar_categorias        → extrai categorias únicas
│
└── TaskGroup: analise
    ├── calcular_metricas_categoria.expand(...)  (fan-out, pool com 2 slots)
    ├── consolidar_metricas                      (fan-in)
    └── salvar_postgres          → UPSERT no banco (idempotente)
```

## Como cada requisito foi atendido

| Requisito | Implementação |
|---|---|
| Rodar às 06:00 (Brasília) | `schedule="0 6 * * *"`, `start_date` com `pendulum.timezone("America/Sao_Paulo")`, `catchup=False` |
| Resistir a instabilidades da API | `retries=4` + `retry_exponential_backoff=True` em `buscar_produtos` |
| Escalar com novas categorias | `listar_categorias()` lê as categorias dinamicamente dos produtos retornados e alimenta `.expand(...)` — nenhuma categoria fixa no código |
| Nunca duplicar ao reprocessar | `INSERT ... ON CONFLICT (categoria, dag_run_id) DO UPDATE` em `salvar_postgres` |
| Avisar quando algo falhar | `on_failure_callback`, `on_retry_callback`, `on_success_callback` em `buscar_produtos` |
| Modular e legível | TaskFlow API, 2 TaskGroups (`ingestao`, `analise`) |
| TaskFlow API / XComs automáticos | tasks decoradas com `@task`; dependências via chamada de função; apenas dados pequenos trafegam (dicionário de métricas por categoria, não a lista de produtos) |
| Dynamic Task Mapping + Pool | `calcular_metricas_categoria.partial(produtos=produtos).expand(categoria=categorias)`, `pool="ecommerce_pool"` (2 slots) |
| Persistência via PostgresHook | `salvar_postgres` usa `PostgresHook` com a Connection `postgres_shopbrasil` |

Os requisitos opcionais (operador customizado de validação, tabela de
histórico em modo append, SLA/alerta) não foram implementados nesta entrega.

## Como rodar

```bash
mkdir -p logs
docker compose up -d
docker compose ps    # aguardar "healthy" em todos os serviços
```

Acesse **http://localhost:8080** (usuário `admin`, senha `admin`).

A Connection `postgres_shopbrasil` e o Pool `ecommerce_pool` (2 slots) são
criados automaticamente pelo serviço `airflow-init`.

Ative o DAG `shopbrasil_metricas_catalogo` e dispare manualmente (▶ Trigger
DAG) para testar.

### Conferir os dados gravados

```bash
docker exec -it shopbrasil-analytics-db psql -U shopbrasil -d shopbrasil_analytics -c "SELECT * FROM metricas_categoria;"
```

### Derrubar o ambiente

```bash
docker compose down       # mantém os volumes
docker compose down -v    # apaga também os dados dos bancos
```
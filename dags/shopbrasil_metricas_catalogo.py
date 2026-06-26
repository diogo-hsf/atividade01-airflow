"""
DAG: shopbrasil_metricas_catalogo

Substitui o script via cron que alimentava o painel matinal do time de
pricing (preço médio, mínimo, máximo e quantidade de produtos por categoria).

Fluxo: FakeStore API -> métricas por categoria -> PostgreSQL, todo dia
às 06:00 (America/Sao_Paulo).

Topologias:
  linear:   buscar_produtos -> listar_categorias
  fan-out:  listar_categorias -> calcular_metricas_categoria.expand(...)
  fan-in:   calcular_metricas_categoria (N tasks) -> consolidar_metricas
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

FAKESTORE_BASE_URL = "https://fakestoreapi.com"
POSTGRES_CONN_ID = "postgres_shopbrasil"
POOL_NAME = "ecommerce_pool"

LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

DEFAULT_ARGS = {
    "owner": "time-dados-shopbrasil",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

def on_failure_callback(context: dict) -> None:
    ti = context["task_instance"]
    log.error("Falha na task '%s' após todas as tentativas.", ti.task_id)


def on_retry_callback(context: dict) -> None:
    ti = context["task_instance"]
    log.warning("Retry da task '%s' (tentativa %s)", ti.task_id, ti.try_number)


def on_success_callback(context: dict) -> None:
    ti = context["task_instance"]
    log.info("Sucesso na task '%s'", ti.task_id)

@dag(
    dag_id="shopbrasil_metricas_catalogo",
    description="Pipeline diário: FakeStore API -> métricas por categoria -> PostgreSQL",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["shopbrasil", "atividade-01", "etl", "pricing"],
    max_active_runs=1,
)
def shopbrasil_metricas_catalogo():

    @task_group(group_id="ingestao")
    def ingestao():

        @task(
            task_id="buscar_produtos",
            retries=4,
            retry_delay=timedelta(seconds=20),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
        )
        def buscar_produtos() -> list[dict]:
            """Busca todos os produtos na FakeStore API."""
            import requests

            url = f"{FAKESTORE_BASE_URL}/products"
            log.info("Buscando produtos em %s", url)

            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                produtos = response.json()
            except requests.exceptions.RequestException as exc:
                log.error("Falha ao chamar a FakeStore API: %s", exc)
                raise AirflowException(f"Erro ao buscar produtos na API: {exc}") from exc

            if not isinstance(produtos, list) or not produtos:
                raise AirflowException("API retornou payload vazio ou em formato inesperado.")

            log.info("%d produtos coletados da FakeStore API", len(produtos))
            return produtos

        @task(task_id="listar_categorias")
        def listar_categorias(produtos: list[dict]) -> list[str]:
            """Extrai categorias únicas; alimenta o fan-out via .expand(...)."""
            categorias = sorted({p["category"] for p in produtos})
            log.info("Categorias encontradas (%d): %s", len(categorias), categorias)
            return categorias

        produtos = buscar_produtos()
        categorias = listar_categorias(produtos)
        return produtos, categorias

    @task_group(group_id="analise")
    def analise(produtos: list[dict], categorias: list[str]):

        @task(task_id="calcular_metricas_categoria", pool=POOL_NAME)
        def calcular_metricas_categoria(categoria: str, produtos: list[dict]) -> dict:
            """Calcula preço médio, mínimo, máximo e quantidade para uma categoria."""
            precos = [p["price"] for p in produtos if p["category"] == categoria]

            if not precos:
                log.warning("Categoria '%s' sem produtos válidos — pulando.", categoria)
                return {
                    "categoria": categoria,
                    "qtd_produtos": 0,
                    "preco_medio": 0.0,
                    "preco_minimo": 0.0,
                    "preco_maximo": 0.0,
                }

            return {
                "categoria": categoria,
                "qtd_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
            }

        metricas_por_categoria = calcular_metricas_categoria.partial(
            produtos=produtos
        ).expand(categoria=categorias)

        @task(task_id="consolidar_metricas")
        def consolidar_metricas(lista_metricas: list[dict]) -> list[dict]:
            """Fan-in: junta o resultado das tasks mapeadas em uma lista só."""
            total_produtos = sum(m["qtd_produtos"] for m in lista_metricas)
            log.info(
                "Consolidação: %d categorias, %d produtos no total",
                len(lista_metricas),
                total_produtos,
            )
            return lista_metricas

        metricas_consolidadas = consolidar_metricas(metricas_por_categoria)

        @task(task_id="salvar_postgres")
        def salvar_postgres(lista_metricas: list[dict], **context) -> int:
            """
            Grava o snapshot via PostgresHook. Idempotente: UPSERT em
            (categoria, dag_run_id) — reprocessar a mesma run atualiza
            em vez de duplicar.
            """
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            run_id = context["run_id"]
            data_execucao = context["ds"]

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            upsert_sql = """
                INSERT INTO metricas_categoria
                    (categoria, qtd_produtos, preco_medio, preco_minimo,
                     preco_maximo, dag_run_id, data_execucao)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (categoria, dag_run_id)
                DO UPDATE SET
                    qtd_produtos  = EXCLUDED.qtd_produtos,
                    preco_medio   = EXCLUDED.preco_medio,
                    preco_minimo  = EXCLUDED.preco_minimo,
                    preco_maximo  = EXCLUDED.preco_maximo,
                    data_execucao = EXCLUDED.data_execucao,
                    atualizado_em = NOW()
            """
            valores = [
                (
                    m["categoria"],
                    m["qtd_produtos"],
                    m["preco_medio"],
                    m["preco_minimo"],
                    m["preco_maximo"],
                    run_id,
                    data_execucao,
                )
                for m in lista_metricas
            ]

            conn = hook.get_conn()
            cur = conn.cursor()
            try:
                cur.executemany(upsert_sql, valores)
                conn.commit()
                log.info(
                    "%d linha(s) gravada(s)/atualizada(s) em 'metricas_categoria' (run_id=%s)",
                    len(valores),
                    run_id,
                )
                return len(valores)
            except Exception as exc:
                conn.rollback()
                log.error("Erro ao gravar métricas no Postgres: %s", exc)
                raise
            finally:
                cur.close()
                conn.close()

        salvar_postgres(metricas_consolidadas)

    produtos, categorias = ingestao()
    analise(produtos, categorias)

dag_instance = shopbrasil_metricas_catalogo()
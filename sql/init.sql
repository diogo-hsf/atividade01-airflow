-- Inicialização do banco analítico (postgres-shopbrasil).
-- Roda automaticamente na primeira subida do container.

-- Idempotência: a constraint única (categoria, dag_run_id) permite o
-- UPSERT usado em salvar_postgres — reprocessar a mesma run atualiza
-- a linha em vez de duplicá-la.
CREATE TABLE IF NOT EXISTS metricas_categoria (
    id              SERIAL PRIMARY KEY,
    categoria       VARCHAR(150) NOT NULL,
    qtd_produtos    INTEGER NOT NULL,
    preco_medio     NUMERIC(12, 2) NOT NULL,
    preco_minimo    NUMERIC(12, 2) NOT NULL,
    preco_maximo    NUMERIC(12, 2) NOT NULL,
    dag_run_id      VARCHAR(250) NOT NULL,
    data_execucao   DATE NOT NULL,
    atualizado_em   TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_categoria_run UNIQUE (categoria, dag_run_id)
);

CREATE INDEX IF NOT EXISTS idx_metricas_categoria_data ON metricas_categoria (data_execucao);

GRANT ALL ON ALL TABLES IN SCHEMA public TO shopbrasil;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO shopbrasil;

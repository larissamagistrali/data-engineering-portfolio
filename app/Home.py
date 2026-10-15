"""Home do portfólio de Engenharia de Dados: visão geral dos cinco projetos."""
import _bootstrap  # noqa: F401
import streamlit as st

from shared import ui

ui.setup_page("Engenharia de Dados · Portfólio", "🛠️")
st.title("Portfólio de Engenharia de Dados")
st.markdown(
    "**Pipelines de dados reais e reproduzíveis, do ingestão ao indicador.** Cada projeto cobre um padrão diferente: "
    "ELT com dbt, lakehouse medalhão em PySpark, orquestração com Airflow, streaming com Kafka e validação de qualidade de dados."
)

st.info(
    "**Pipeline roda localmente, app mostra o resultado.** Airflow, Kafka, Spark e dbt não rodam no Streamlit Cloud. "
    "Cada projeto executa o pipeline na máquina (CLI ou docker-compose), exporta uma camada gold pequena "
    "(parquet/JSON versionados em `projects/*/data/`) e este app apenas lê esses arquivos.",
    icon="🖥️",
)

st.markdown(
    "**Organização do repositório:** `projects/<projeto>/` tem o pacote Python (`src/`), os testes (`tests/`), "
    "as amostras e agregados que o app lê (`data/`) e um README com problema, dados, abordagem, resultados e limitações. "
    "`shared/` contém o tema visual e a estrutura de página comuns; `app/` é o app Streamlit multipágina."
)

st.subheader("Projetos")
ui.project_grid([
    {
        "title": "Olist · dbt e DuckDB", "icon": "🧱", "page": "pages/1_Olist_DBT.py",
        "summary": "Warehouse dimensional com dbt: staging, intermediate, marts, KPIs gold e 139 testes de dados.",
        "stack": "dbt-core, dbt-duckdb, DuckDB, SQL", "data_kind": "synthetic",
        "dataset": "schema Olist, gerado de forma sintética (original é só Kaggle)",
    },
    {
        "title": "NYC Taxi · Lakehouse", "icon": "🚕", "page": "pages/2_NYC_Taxi_Lakehouse.py",
        "summary": "Medalhão bronze, silver e gold em PySpark sobre as corridas de táxi amarelo de NYC, com rejeições por regra.",
        "stack": "PySpark, Parquet, arquitetura medalhão", "data_kind": "real",
        "dataset": "NYC TLC Yellow Taxi 2024",
    },
    {
        "title": "BCB · Airflow", "icon": "🏦", "page": "pages/3_BCB_Airflow.py",
        "summary": "Ingestão incremental de séries do Banco Central (Selic, IPCA, câmbio) com DAG, retries e validação.",
        "stack": "Apache Airflow (TaskFlow), Python, Parquet", "data_kind": "real",
        "dataset": "API SGS do Banco Central",
    },
    {
        "title": "Wikipedia · Streaming", "icon": "📡", "page": "pages/4_Wikipedia_Streaming.py",
        "summary": "Edições da Wikipedia em tempo real: SSE para Kafka, agregação em janelas de 1 minuto e latência medida.",
        "stack": "Apache Kafka (KRaft), Python, janelas e watermark", "data_kind": "real",
        "dataset": "Wikimedia EventStreams",
    },
    {
        "title": "Qualidade de dados", "icon": "✅", "page": "pages/5_Data_Quality.py",
        "summary": "Framework próprio de regras com severidade e dimensões, aplicado a contratações públicas do PNCP.",
        "stack": "Python, pandas, pytest (regras próprias)", "data_kind": "real",
        "dataset": "PNCP, API de consulta pública",
    },
], columns=2)

st.caption("Escolha um projeto no menu lateral ou nos cartões acima. Cada aba segue o roteiro: Problema · Dados · Abordagem · Resultado · Demo interativa.")

# Portfólio de Engenharia de Dados

Cinco projetos de pipelines de dados, da ingestão à camada de consumo: dbt e DuckDB, lakehouse medalhão em PySpark,
orquestração com Airflow, streaming com Kafka e validação de qualidade de dados. Cada projeto tem README próprio com
problema, dados, abordagem, resultados e limitações.

**App no ar:** https://SEU-APP.streamlit.app (TODO: preencher após o deploy)

**Regra de ouro:** o pipeline roda localmente (CLI ou docker-compose) e exporta uma camada gold pequena em
parquet/JSON. O app Streamlit só lê esse resultado. Airflow, Kafka, Spark e dbt não rodam no Streamlit Cloud.

## Projetos

| Projeto | Pergunta que responde | Dados | Stack | Status |
|---|---|---|---|---|
| [olist-dbt-warehouse](projects/olist-dbt-warehouse/README.md) | Como modelar um marketplace em camadas testadas e chegar a KPIs auditáveis? | sintéticos (schema Olist) | dbt-core, dbt-duckdb, DuckDB | `dbt build` executado: 163 nós ok, 1 aviso intencional, 0 erros |
| [nyc-taxi-lakehouse](projects/nyc-taxi-lakehouse/README.md) | Como organizar corridas de táxi em bronze, silver e gold com rejeições por regra? | real (NYC TLC jan–fev/2024) | PySpark, Parquet | pipeline Spark executado: 5,97 mi linhas na bronze, 5,44 mi na silver |
| [bcb-airflow-ingestion](projects/bcb-airflow-ingestion/README.md) | Como ingerir séries do Banco Central de forma incremental e idempotente? | real (API SGS/BCB) | Airflow, Python, Parquet | DAG executada com `airflow dags test` (carga inicial e incremental); docker-compose não executado |
| [wikipedia-streaming](projects/wikipedia-streaming/README.md) | Como agregar edições da Wikipedia em tempo real e medir a latência ponta a ponta? | real (Wikimedia EventStreams) | Kafka (KRaft), Python | captura real de 5 min com Kafka local; docker-compose não executado |
| [data-quality](projects/data-quality/README.md) | Quão confiáveis são os dados de contratações públicas segundo regras declarativas? | real (PNCP) | Python, pandas, pytest | 25 regras aplicadas a 6.000 contratos reais |

## Como rodar

```bash
make install     # cria .venv e instala as dependências leves do app
make app         # sobe o Streamlit em http://localhost:8501
make test        # pytest na raiz: smoke test de todas as páginas + testes dos projetos
make data        # roda todos os pipelines locais e atualiza os artefatos em projects/*/data
make help        # lista os alvos por projeto (data-<projeto>)
```

Os pipelines pesados (dbt, Spark, Airflow, Kafka) têm requirements próprios em `projects/<projeto>/requirements.txt`
e não entram no `requirements.txt` raiz, que é só do app.

## Deploy no Streamlit Community Cloud

- Arquivo principal: `app/Home.py`
- Python 3.11 ou superior
- Dependências: `requirements.txt` da raiz (o app é leve, com ~1 GB de RAM)
- Segredos: `.streamlit/secrets.toml` local; no Cloud, em Settings > Secrets (o app atual não usa segredos)

## Estrutura

```
app/                 app Streamlit multipágina (Home + uma página por projeto)
shared/              tema visual e estrutura de página comuns aos repositórios
projects/<projeto>/  src/ (pacote), tests/, data/ (amostras e agregados), README.md
tests/               smoke test do app (AppTest sobre Home e cada página)
.github/workflows/   CI: instala as dependências do app e roda pytest
```

## Origem dos dados

- **Reais:** NYC TLC Yellow Taxi, API SGS do Banco Central, Wikimedia EventStreams e API de consulta pública do PNCP.
  Cada projeto cita endpoint, período coletado e termos de uso.
- **Sintéticos:** Olist. O dataset original é só do Kaggle (CC BY-NC-SA 4.0), que exige credencial. O projeto gera dados
  com o mesmo schema e rotula tudo como sintético.
- Nenhum dado pessoal identificável é versionado. Números de cada README vêm de execuções reais dos pipelines.

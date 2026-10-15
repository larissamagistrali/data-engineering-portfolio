# Atalhos do repositório. `make help` lista os alvos.
PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: help install app test data data-olist-dbt-warehouse data-bcb-airflow-ingestion data-data-quality data-nyc-taxi-lakehouse data-wikipedia-streaming
help:            ## Lista os alvos
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-22s %s\n", $$1, $$2}'

install:         ## Cria o venv e instala as dependências do app
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -r requirements.txt -r requirements-dev.txt

app:             ## Sobe o app Streamlit
	$(BIN)/streamlit run app/Home.py

test:            ## Roda os testes (inclui o teste de fumaça do app)
	$(BIN)/pytest -q

# Pipelines locais: venvs pesados por projeto, fora do venv do app.
PY_OLIST ?= $(HOME)/.venvs/de-olist/bin/python

PY_NYC ?= $(HOME)/.venvs/de-nyc/bin/python
AIRFLOW_BIN ?= $(HOME)/.venvs/de-bcb/bin/airflow
BCB_AIRFLOW_ENV = AIRFLOW_HOME=$(HOME)/airflow-bcb \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/projects/bcb-airflow-ingestion/dags \
	PYTHONPATH=$(CURDIR)/projects/bcb-airflow-ingestion/src \
	BCB_DATA_DIR=$(CURDIR)/projects/bcb-airflow-ingestion/data

data:            ## Roda todos os pipelines locais e atualiza projects/*/data
	$(MAKE) data-olist-dbt-warehouse
	$(MAKE) data-nyc-taxi-lakehouse
	$(MAKE) data-bcb-airflow-ingestion
	$(MAKE) data-data-quality

data-olist-dbt-warehouse:  ## Olist: gera CSVs sintéticos, carrega raw, dbt build e exporta gold
	PYTHONPATH=projects/olist-dbt-warehouse/src $(PY_OLIST) -m olist_warehouse.pipeline

# Requer PySpark + Java 17 no venv e os parquets brutos em projects/nyc-taxi-lakehouse/data/raw/tlc/ (URLs no README)
data-nyc-taxi-lakehouse:  ## NYC Taxi: Spark bronze -> silver -> gold e exporta o gold para data/
	PYTHONPATH=projects/nyc-taxi-lakehouse/src $(PY_NYC) -m nyc_taxi_lakehouse.pipeline

data-bcb-airflow-ingestion:  ## BCB: executa a DAG bcb_ingestion (airflow dags test) para hoje
	$(BCB_AIRFLOW_ENV) $(AIRFLOW_BIN) db migrate
	$(BCB_AIRFLOW_ENV) $(AIRFLOW_BIN) dags test bcb_ingestion $$(date -u +%F)

# Requer as deps do pipeline no venv: $(BIN)/pip install -r projects/data-quality/requirements.txt
data-data-quality:  ## Qualidade: coleta contratos do PNCP (rede) e roda as 25 regras
	PYTHONPATH=projects/data-quality/src $(BIN)/python -m data_quality.pipeline --coletar

# --- Alvos por projeto (um por projeto, ex.: `data-churn-prediction`) ficam abaixo ---

# Requer o venv do projeto (projects/wikipedia-streaming/requirements.txt) e o Kafka rodando em localhost:9092
# com o tópico wiki-recentchange criado (passos no README do projeto).
PY_WIKI ?= $(HOME)/.venvs/de-wiki/bin/python
WIKI_STATS = projects/wikipedia-streaming/data/raw/producer-stats.json

data-wikipedia-streaming:  ## Wikipedia: captura ~5 min do EventStreams via Kafka e agrega por minuto
	mkdir -p $(dir $(WIKI_STATS))
	PYTHONPATH=projects/wikipedia-streaming/src $(PY_WIKI) -m wikipedia_streaming.aggregator --run-seconds 310 --producer-stats $(WIKI_STATS) & \
	sleep 4; \
	PYTHONPATH=projects/wikipedia-streaming/src $(PY_WIKI) -m wikipedia_streaming.producer --duration 300 --stats-out $(WIKI_STATS); \
	wait

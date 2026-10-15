"""DAG diária de ingestão do SGS (Banco Central). Só orquestra: a lógica está em `bcb_ingestion`."""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

from bcb_ingestion import pipeline
from bcb_ingestion.client import SERIES

DATA_DIR = Path(os.environ.get("BCB_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))


@dag(
    dag_id="bcb_ingestion",
    description="Ingestão incremental de séries do SGS/BCB (bronze -> silver -> gold)",
    schedule="0 9 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,  # não retroage: a carga incremental cobre o intervalo faltante a cada execução
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["portfolio", "bcb", "sgs"],
)
def bcb_ingestion():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(max_active_tis_per_dagrun=3)
    def fetch(codigo: int) -> dict:
        return pipeline.fetch_series(codigo, DATA_DIR, today=date.today())

    @task
    def validate(run: dict) -> dict:
        return pipeline.validate_series(run["codigo"], DATA_DIR, today=date.today())

    @task
    def write(runs: list[dict], validations: list[dict]) -> dict:
        return pipeline.write_outputs(runs, DATA_DIR)

    runs = fetch.expand(codigo=list(SERIES))
    checks = validate.expand(run=runs)
    written = write(runs=runs, validations=checks)
    start >> runs
    written >> end


bcb_ingestion()

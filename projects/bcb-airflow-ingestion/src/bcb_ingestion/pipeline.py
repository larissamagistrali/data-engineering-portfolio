"""Orquestração pura: as tarefas que a DAG chama (uma por série) e um runner sequencial local."""
from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .client import SERIES, build_url, fetch_json, parse_observations, windows
from .incremental import last_date, next_window_start
from .storage import build_gold, read_silver, write_bronze, write_json, write_parquet, write_silver
from .validate import validate_observations


def _dirs(data_dir: Path) -> tuple[Path, Path, Path]:
    raw = data_dir / "raw"
    return raw / "bronze", raw / "silver", data_dir


def fetch_series(
    codigo: int,
    data_dir: Path,
    today: date,
    session: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Tarefa 'fetch': coleta só o que falta (incremental) e atualiza a silver da série."""
    bronze_dir, silver_dir, _ = _dirs(data_dir)
    session = session or requests.Session()
    inicio = next_window_start(last_date(read_silver(codigo, silver_dir)))
    frames = []
    for ini, fim in windows(inicio, today):
        payload = fetch_json(session, build_url(codigo, ini, fim), sleep=sleep)
        write_bronze(codigo, ini.isoformat(), fim.isoformat(), payload, bronze_dir)
        frames.append(parse_observations(payload, codigo))
    new = pd.concat(frames, ignore_index=True) if frames else parse_observations([], codigo)
    _, n_novas = write_silver(codigo, new, silver_dir)
    return {"codigo": codigo, "serie": SERIES[codigo], "desde": inicio.isoformat(),
            "linhas_recebidas": len(new), "linhas_novas": n_novas}


def validate_series(codigo: int, data_dir: Path, today: date | None = None) -> dict[str, Any]:
    """Tarefa 'validate': falha a tarefa se a silver da série tiver problemas."""
    _, silver_dir, _ = _dirs(data_dir)
    df = read_silver(codigo, silver_dir)
    problemas = validate_observations(df if df is not None else pd.DataFrame(), today=today)
    if problemas:
        raise ValueError(f"série {codigo} ({SERIES[codigo]}) inválida: {'; '.join(problemas)}")
    return {"codigo": codigo, "serie": SERIES[codigo], "linhas": len(df), "problemas": 0}


def write_outputs(runs: list[dict[str, Any]], data_dir: Path, generated_at: datetime | None = None) -> dict[str, Any]:
    """Tarefa 'write': grava series_gold.parquet e series_status.json (sobrescrita atômica)."""
    _, silver_dir, out_dir = _dirs(data_dir)
    gold = build_gold(silver_dir)
    write_parquet(gold, out_dir / "series_gold.parquet")

    ts = (generated_at or datetime.now(timezone.utc)).isoformat(timespec="seconds")
    by_code = {r["codigo"]: r for r in runs}
    series = []
    for codigo, nome in SERIES.items():
        sub = gold[gold["codigo"] == codigo]
        run = by_code.get(codigo, {})
        series.append({
            "codigo": codigo,
            "serie": nome,
            "primeira_data": sub["data"].min().date().isoformat(),
            "ultima_data": sub["data"].max().date().isoformat(),
            "linhas": int(len(sub)),
            "linhas_novas_na_execucao": int(run.get("linhas_novas", 0)),
        })
    status = {"gerado_em_utc": ts, "fonte": "API SGS do Banco Central (dados públicos)", "series": series}
    write_json(status, out_dir / "series_status.json")
    return {"linhas_gold": int(len(gold)), "series": len(series)}


def run_all(data_dir: Path, today: date, session: Any | None = None,
            sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Execução sequencial sem Airflow (mesma ordem da DAG: fetch -> validate -> write)."""
    runs = [fetch_series(c, data_dir, today, session=session, sleep=sleep) for c in SERIES]
    for c in SERIES:
        validate_series(c, data_dir, today=today)
    return write_outputs(runs, data_dir)

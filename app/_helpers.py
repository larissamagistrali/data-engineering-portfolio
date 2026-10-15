"""Utilidades locais do app (o `shared/` é idêntico entre os repos e não deve ser editado)."""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import streamlit as st

LOCAL_PIPELINE_NOTE = (
    "**Pipeline roda localmente, app mostra o resultado.** Airflow, Kafka, Spark e dbt não rodam no Streamlit Cloud: "
    "cada projeto executa o pipeline na máquina (docker-compose ou CLI), exporta uma camada gold pequena "
    "(parquet/JSON versionados em `projects/*/data/`) e este app apenas lê esses arquivos."
)

# status -> (ícone, rótulo): cor nunca é o único sinal
STATUS_LABELS = {
    "pass": ("✅", "Passou"),
    "success": ("✅", "Sucesso"),
    "warn": ("⚠️", "Aviso"),
    "fail": ("❌", "Falhou"),
    "error": ("❌", "Erro"),
    "skipped": ("⏭️", "Ignorado"),
    "running": ("⏳", "Em execução"),
    "failed": ("❌", "Falhou"),
    "up_for_retry": ("🔁", "Nova tentativa"),
    "upstream_failed": ("⛔", "Upstream falhou"),
}


def status_label(status: str) -> str:
    """Ícone + texto para um status (ex.: '✅ Passou')."""
    icon, label = STATUS_LABELS.get(str(status).lower(), ("•", str(status)))
    return f"{icon} {label}"


def pipeline_note() -> None:
    st.info(LOCAL_PIPELINE_NOTE, icon="🖥️")


def missing_data(paths: Iterable[Path], hint: str) -> bool:
    """Mostra um aviso e devolve True se algum artefato esperado não existe."""
    absent = [p.name for p in paths if not Path(p).exists()]
    if absent:
        st.warning(f"Artefatos ausentes: {', '.join(absent)}. {hint}")
    return bool(absent)


def fmt_brl(value: float) -> str:
    """R$ 1.234,56 (formato brasileiro)."""
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def fmt_int(value: float) -> str:
    return f"{int(value):,}".replace(",", ".")


def show_table(df: pd.DataFrame, height: int | None = None) -> None:
    kwargs = {"height": height} if height else {}
    st.dataframe(df, hide_index=True, width="stretch", **kwargs)

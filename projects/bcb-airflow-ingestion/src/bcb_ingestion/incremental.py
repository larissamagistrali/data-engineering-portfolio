"""Carga incremental: a partir da última data já gravada na silver."""
from __future__ import annotations

from datetime import date

import pandas as pd

DEFAULT_START = date(2010, 1, 1)


def last_date(silver: pd.DataFrame | None) -> date | None:
    """Maior data já carregada, ou None se a série ainda não existe."""
    if silver is None or silver.empty:
        return None
    return pd.to_datetime(silver["data"]).max().date()


def next_window_start(last: date | None, default: date = DEFAULT_START) -> date:
    """Ponto de partida da coleta.

    Reprocessa a última data (inclusive) para capturar revisões do BCB; a deduplicação
    na escrita evita linhas repetidas.
    """
    return last if last is not None else default

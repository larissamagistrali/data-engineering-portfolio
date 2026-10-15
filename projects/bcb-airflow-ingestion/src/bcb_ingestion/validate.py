"""Checagens de qualidade sobre as observações de uma série antes da camada gold."""
from __future__ import annotations

from datetime import date

import pandas as pd
from pandas.api.types import is_numeric_dtype

from .client import COLUMNS


def validate_observations(df: pd.DataFrame, today: date | None = None) -> list[str]:
    """Devolve a lista de problemas encontrados (vazia = válido)."""
    if df.empty:
        return ["série sem observações"]
    faltando = [c for c in COLUMNS if c not in df.columns]
    if faltando:
        return [f"colunas ausentes: {', '.join(faltando)}"]

    problemas: list[str] = []
    if df["data"].isna().any():
        problemas.append("datas nulas")
    if not is_numeric_dtype(df["valor"]):
        problemas.append("valor não numérico")
    elif df["valor"].isna().any():
        problemas.append(f"{int(df['valor'].isna().sum())} valores nulos ou inválidos")
    if df.duplicated(["codigo", "data"]).any():
        problemas.append("datas duplicadas para a mesma série")
    if today is not None and (pd.to_datetime(df["data"]) > pd.Timestamp(today)).any():
        problemas.append("datas no futuro")
    return problemas

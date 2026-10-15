"""Escrita idempotente das camadas bronze (JSON bruto), silver (por série) e gold (consolidada)."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .client import COLUMNS, SERIES

KEYS = ["codigo", "data"]


def _atomic_replace(tmp: Path, dest: Path) -> None:
    os.replace(tmp, dest)


def write_parquet(df: pd.DataFrame, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    _atomic_replace(tmp, dest)


def write_json(obj: Any, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _atomic_replace(tmp, dest)


def write_bronze(codigo: int, inicio: str, fim: str, payload: list[dict], bronze_dir: Path) -> Path:
    """Guarda a resposta bruta de uma janela (auditoria e reprocessamento)."""
    dest = bronze_dir / f"{codigo}_{inicio}_{fim}.json"
    write_json(payload, dest)
    return dest


def read_silver(codigo: int, silver_dir: Path) -> pd.DataFrame | None:
    path = silver_dir / f"{codigo}.parquet"
    return pd.read_parquet(path) if path.exists() else None


def merge_observations(old: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """Une históricos: uma linha por (codigo, data), com a versão mais recente vencendo."""
    frames = [f for f in (old, new) if f is not None and not f.empty]
    if not frames:
        return new.iloc[0:0].copy()
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=KEYS, keep="last")
    return merged.sort_values(KEYS).reset_index(drop=True)[COLUMNS]


def write_silver(codigo: int, new: pd.DataFrame, silver_dir: Path) -> tuple[Path, int]:
    """Grava a silver da série. Devolve (caminho, número de linhas novas). Reexecutar é no-op."""
    old = read_silver(codigo, silver_dir)
    merged = merge_observations(old, new)
    n_novas = len(merged) - (0 if old is None else len(old))
    dest = silver_dir / f"{codigo}.parquet"
    write_parquet(merged, dest)
    return dest, n_novas


def build_gold(silver_dir: Path) -> pd.DataFrame:
    """Une a silver de todas as séries conhecidas em formato longo (data, serie, codigo, valor)."""
    frames = [read_silver(c, silver_dir) for c in SERIES]
    frames = [f for f in frames if f is not None]
    if not frames:
        raise FileNotFoundError(f"nenhuma silver em {silver_dir}")
    return pd.concat(frames, ignore_index=True).sort_values(["codigo", "data"]).reset_index(drop=True)[COLUMNS]

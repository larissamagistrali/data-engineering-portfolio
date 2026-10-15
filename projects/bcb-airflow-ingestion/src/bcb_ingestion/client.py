"""Cliente HTTP da API SGS do Banco Central: URL, janelas, retry com backoff e parse."""
from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
USER_AGENT = "portfolio-demo/0.1 (https://github.com/larissamagistrali)"

# Código SGS -> nome curto usado nos arquivos e no app. Validados contra a API em 2026-10.
SERIES: dict[int, str] = {
    432: "selic_meta",
    1: "dolar_venda",
    433: "ipca_mensal",
    4189: "selic_over_mensal",
    189: "igpm_mensal",
}

# A API aceita no máximo 10 anos por consulta para séries diárias; 5 anos dá folga.
WINDOW_YEARS = 5
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
COLUMNS = ["data", "serie", "codigo", "valor"]


def build_url(codigo: int, inicio: date, fim: date) -> str:
    """URL da consulta de uma janela (datas no formato dd/mm/aaaa exigido pela API)."""
    return (
        f"{BASE_URL.format(codigo=codigo)}?formato=json"
        f"&dataInicial={inicio:%d/%m/%Y}&dataFinal={fim:%d/%m/%Y}"
    )


def windows(inicio: date, fim: date, anos: int = WINDOW_YEARS) -> list[tuple[date, date]]:
    """Quebra [inicio, fim] (fim inclusivo) em janelas de no máximo `anos` anos."""
    out: list[tuple[date, date]] = []
    cur = inicio
    while cur <= fim:
        nxt = min(cur + timedelta(days=365 * anos - 1), fim)
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out


def fetch_json(
    session: Any,
    url: str,
    retries: int = 3,
    backoff: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict]:
    """GET com User-Agent descritivo; repete em 429/5xx e erros de rede, com backoff exponencial."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            erro: Exception = exc
        else:
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in RETRY_STATUS:
                # 400/404 etc.: erro definitivo, não adianta repetir
                raise requests.HTTPError(f"HTTP {resp.status_code} em {url}")
            erro = requests.HTTPError(f"HTTP {resp.status_code} em {url}")
        if attempt == retries:
            raise erro
        sleep(backoff * 2**attempt)
    raise AssertionError("inalcançável")  # pragma: no cover


def parse_observations(payload: list[dict], codigo: int) -> pd.DataFrame:
    """Converte a resposta JSON do SGS em DataFrame tipado (data, serie, codigo, valor)."""
    serie = SERIES.get(codigo, str(codigo))
    if not payload:
        return pd.DataFrame({"data": pd.Series(dtype="datetime64[ns]"), "serie": pd.Series(dtype="str"),
                             "codigo": pd.Series(dtype="int64"), "valor": pd.Series(dtype="float64")})
    raw = pd.DataFrame(payload)
    return pd.DataFrame({
        "data": pd.to_datetime(raw["data"], format="%d/%m/%Y"),
        "serie": serie,
        "codigo": codigo,
        "valor": pd.to_numeric(raw["valor"], errors="coerce").astype("float64"),
    })[COLUMNS]

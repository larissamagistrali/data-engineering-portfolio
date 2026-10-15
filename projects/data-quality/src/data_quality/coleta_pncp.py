"""Coleta de contratos publicados no PNCP (API pública de consulta, sem autenticação).

Endpoint: GET https://pncp.gov.br/api/consulta/v1/contratos (por data de publicação, paginado).
A amostra é por páginas sorteadas com semente fixa (cluster sampling), não linhas soltas.
Cada página vai para o cache como JSON (`*.part` renomeado ao final) e é reutilizada se existir.
"""
from __future__ import annotations

import json
import random
import time
from datetime import date
from pathlib import Path
from typing import Any

import requests

ENDPOINT = "https://pncp.gov.br/api/consulta/v1/contratos"
USER_AGENT = "portfolio-demo/0.1 (https://github.com/larissamagistrali)"
JANELA = ("20250101", "20250331")  # datas de publicação, yyyyMMdd
TAMANHO_PAGINA = 500  # máximo aceito pela API
PAGINAS_AMOSTRADAS = 12  # 12 x 500 = 6.000 registros
SEMENTE = 42
PAUSA_S = 1.0  # respeito ao rate limit


def _get(params: dict[str, Any]) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for tentativa in range(4):
        resp = requests.get(ENDPOINT, params=params, headers=headers, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and tentativa < 3:
            time.sleep(5 * (tentativa + 1))
            continue
        resp.raise_for_status()
    raise RuntimeError("PNCP: tentativas esgotadas")


def _pagina(cache: Path, pagina: int, janela: tuple[str, str]) -> dict[str, Any]:
    final = cache / f"contratos_{janela[0]}_{janela[1]}_p{pagina:05d}.json"
    if final.exists():
        return json.loads(final.read_text(encoding="utf-8"))
    params = {"dataInicial": janela[0], "dataFinal": janela[1], "pagina": pagina, "tamanhoPagina": TAMANHO_PAGINA}
    dados = _get(params)
    parte = final.with_suffix(".part")
    parte.write_text(json.dumps(dados, ensure_ascii=False), encoding="utf-8")
    parte.rename(final)
    time.sleep(PAUSA_S)
    return dados


def coletar(cache: Path, janela: tuple[str, str] = JANELA, n_paginas: int = PAGINAS_AMOSTRADAS) -> list[dict[str, Any]]:
    """Baixa (ou reutiliza do cache) n_paginas páginas sorteadas e devolve os registros brutos."""
    cache.mkdir(parents=True, exist_ok=True)
    primeira = _pagina(cache, 1, janela)
    total_paginas = int(primeira["totalPaginas"])
    rng = random.Random(SEMENTE)
    escolhidas = sorted(rng.sample(range(1, total_paginas + 1), min(n_paginas, total_paginas)))
    registros: list[dict[str, Any]] = []
    for pagina in escolhidas:
        registros.extend(_pagina(cache, pagina, janela)["data"])
    (cache / "manifesto.json").write_text(
        json.dumps(
            {
                "endpoint": ENDPOINT,
                "janela_publicacao": list(janela),
                "tamanho_pagina": TAMANHO_PAGINA,
                "total_registros_janela": primeira["totalRegistros"],
                "total_paginas_janela": total_paginas,
                "paginas_amostradas": escolhidas,
                "semente": SEMENTE,
                "registros_coletados": len(registros),
                "coletado_em": date.today().isoformat(),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return registros


def carregar_cache(cache: Path) -> list[dict[str, Any]]:
    """Lê os registros das páginas amostradas (lista do manifesto) de um cache já coletado, sem rede."""
    manifesto = json.loads((cache / "manifesto.json").read_text(encoding="utf-8"))
    registros: list[dict[str, Any]] = []
    for pagina in manifesto["paginas_amostradas"]:
        janela = manifesto["janela_publicacao"]
        arq = cache / f"contratos_{janela[0]}_{janela[1]}_p{pagina:05d}.json"
        registros.extend(json.loads(arq.read_text(encoding="utf-8"))["data"])
    return registros

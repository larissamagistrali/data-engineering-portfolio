"""Pipeline: (coleta opcional) -> normaliza -> valida -> exporta relatório e amostra de falhas.

Uso (a partir de projects/data-quality):
    PYTHONPATH=src python -m data_quality.pipeline                 # usa o cache já coletado
    PYTHONPATH=src python -m data_quality.pipeline --coletar       # consulta a API do PNCP antes
Saída: data/relatorio_qualidade.json e data/linhas_com_falha.parquet (o app lê só isso).
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import regras as R
from .catalogo import regras_contratos
from .coleta_pncp import JANELA, PAGINAS_AMOSTRADAS, ENDPOINT, carregar_cache, coletar
from .normalizar import mascarar_pessoa_fisica, normalizar

PROJETO = Path(__file__).resolve().parents[2]
DATA_DIR = PROJETO / "data"
CACHE_PADRAO = DATA_DIR / "raw" / "pncp"
MAX_LINHAS_FALHA = 300
SEMENTE_AMOSTRA = 42
COLUNAS_FALHA = [
    "numero_controle_pncp", "cnpj_orgao", "razao_social_orgao", "uf_sigla", "tipo_pessoa",
    "ni_fornecedor", "nome_fornecedor", "valor_global", "data_publicacao",
    "data_vigencia_inicio", "data_vigencia_fim", "objeto_contrato",
]


def _manifesto(cache: Path) -> dict[str, Any]:
    return json.loads((cache / "manifesto.json").read_text(encoding="utf-8"))


def _pct(x: float) -> float:
    return round(100 * float(x), 2)


def _tabela_falhas(df: pd.DataFrame, res: dict[str, Any]) -> pd.DataFrame:
    """Linhas com falha: todas as que têm erro primeiro, depois amostra de avisos (semente fixa), mascaradas."""
    falhas = pd.DataFrame(res["falhas_por_regra"]).reindex(df.index).fillna(False).astype(bool)
    com_falha = df.index[falhas.any(axis=1)]
    com_erro = df.index[res["linhas_com_erro"]]
    so_aviso = com_falha.difference(com_erro)
    espaco = max(MAX_LINHAS_FALHA - len(com_erro), 0)
    if len(so_aviso) > espaco:
        so_aviso = pd.Index(np.random.default_rng(SEMENTE_AMOSTRA).choice(so_aviso.to_numpy(), espaco, replace=False))
    ids = com_erro[:MAX_LINHAS_FALHA].append(so_aviso)
    base = mascarar_pessoa_fisica(df.loc[ids])
    saida = base[[c for c in COLUNAS_FALHA if c in base.columns]].copy()
    saida["objeto_contrato"] = saida["objeto_contrato"].astype("string").str.slice(0, 120)
    saida.insert(0, "regras_falhas", [", ".join(falhas.columns[falhas.loc[i]]) for i in ids])
    saida.insert(0, "severidade_maxima", ["erro" if i in com_erro else "aviso" for i in ids])
    return saida.reset_index(drop=True)


def executar(cache: Path, saida: Path = DATA_DIR) -> dict[str, Any]:
    """Valida o cache de contratos e escreve o relatório JSON e a amostra de linhas com falha."""
    registros = carregar_cache(cache)
    if not registros:
        raise SystemExit(f"Sem registros em {cache}. Rode com --coletar (precisa de rede).")
    manifesto = _manifesto(cache)
    data_ref = pd.Timestamp(manifesto.get("coletado_em", date.today().isoformat()))
    df = normalizar(registros)
    regras = regras_contratos(data_ref)
    res = R.avaliar(df, regras, mascarar=mascarar_pessoa_fisica)

    resultados = res["regras"]
    avaliadas = [r for r in resultados if r["status"] != "ignorada"]
    aprov = [r["aprovacao"] for r in avaliadas if r["aprovacao"] is not None]
    relatorio = {
        "dataset": "contratos_pncp",
        "tipo_dados": "real",
        "fonte": {
            "nome": "PNCP - Portal Nacional de Contratações Públicas (API pública de consulta)",
            "endpoint": ENDPOINT,
            "parametros": {"consulta": "contratos por data de publicação", "dataInicial": JANELA[0],
                           "dataFinal": JANELA[1], "tamanhoPagina": manifesto["tamanho_pagina"]},
            "periodo_publicacao": [JANELA[0], JANELA[1]],
            "total_registros_na_janela": manifesto["total_registros_janela"],
            "total_paginas_na_janela": manifesto["total_paginas_janela"],
            "amostragem": f"{PAGINAS_AMOSTRADAS} páginas sorteadas (semente {manifesto['semente']}) "
                          "de todas as páginas da janela; amostra por cluster de páginas, não linhas isoladas",
            "paginas_amostradas": manifesto["paginas_amostradas"],
            "coletado_em": manifesto.get("coletado_em"),
            "licenca": "dados públicos de acesso livre do PNCP; citar a fonte",
        },
        "data_referencia": data_ref.date().isoformat(),
        "registros": int(len(df)),
        "regras_total": len(regras),
        "regras_avaliadas": len(avaliadas),
        "regras_passaram": sum(1 for r in avaliadas if r["status"] == "passou"),
        "score_geral_pct": _pct(np.mean(aprov)) if aprov else None,
        "registros_sem_erro_pct": _pct(1 - res["linhas_com_erro"].mean()),
        "registros_com_erro": int(res["linhas_com_erro"].sum()),
        "registros_com_aviso_ou_erro": int(pd.DataFrame(res["falhas_por_regra"]).any(axis=1).sum()),
        "dimensoes": R.score_dimensoes(resultados),
        "regras": resultados,
    }
    saida.mkdir(parents=True, exist_ok=True)
    (saida / "relatorio_qualidade.json").write_text(
        json.dumps(relatorio, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    falhas = _tabela_falhas(df, res)
    falhas.to_parquet(saida / "linhas_com_falha.parquet", compression="zstd", index=False)
    return relatorio


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Valida contratos do PNCP e exporta o relatório.")
    parser.add_argument("--coletar", action="store_true", help="consulta a API do PNCP antes de validar")
    parser.add_argument("--cache", type=Path, default=CACHE_PADRAO, help="diretório do cache de páginas")
    parser.add_argument("--saida", type=Path, default=DATA_DIR, help="diretório de saída (data/)")
    args = parser.parse_args(argv)
    if args.coletar:
        registros = coletar(args.cache)
        print(f"coletados {len(registros)} registros em {args.cache}")
    rel = executar(args.cache, args.saida)
    print(f"registros={rel['registros']} regras={rel['regras_total']} avaliadas={rel['regras_avaliadas']} "
          f"score_geral={rel['score_geral_pct']}% sem_erro={rel['registros_sem_erro_pct']}%")


if __name__ == "__main__":
    main()

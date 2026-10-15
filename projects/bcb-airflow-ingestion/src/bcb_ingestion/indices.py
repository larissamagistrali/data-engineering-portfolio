"""Como comparar séries de naturezas diferentes (nível, variação mensal, taxa).

Só pandas: o app importa este módulo sem as dependências do pipeline.
"""
from __future__ import annotations

import pandas as pd

# Natureza de cada série (conferida pela ordem de grandeza: em jan/2010, 4189 = 8,65 com a meta em 8,75).
UNIDADES: dict[str, str] = {
    "selic_meta": "% a.a.",  # 432: meta da Selic definida pelo Copom (taxa)
    "dolar_venda": "R$/US$",  # 1: câmbio comercial de venda (nível)
    "ipca_mensal": "% a.m.",  # 433: variação mensal do IPCA
    "selic_over_mensal": "% a.a.",  # 4189: Selic acumulada no mês, anualizada base 252 (taxa)
    "igpm_mensal": "% a.m.",  # 189: variação mensal do IGP-M
}


def nivel_comparavel(valores: pd.Series, unidade: str) -> pd.Series | None:
    """Série em nível que admite índice base 100, ou None.

    Variação mensal vira índice acumulado, produto de (1 + r/100): indexar a própria taxa daria
    números sem sentido (0,3% -> 0,6% vira "200"). Níveis passam direto. Taxas em nível ficam
    fora: base 100 de uma taxa mede a variação da própria taxa, não se compara a um índice de preços.
    """
    if unidade == "% a.m.":
        return (1.0 + valores / 100.0).cumprod()
    if unidade.startswith("%"):
        return None
    return valores


def variacao_no_periodo(valores: pd.Series, unidade: str) -> tuple[float, str]:
    """Variação entre o primeiro e o último ponto, na métrica que faz sentido para a série.

    Nível: variação percentual. Variação mensal: acumulado no período (%). Taxa: diferença em p.p.
    """
    v = valores.dropna()
    if unidade == "% a.m.":
        return float(((1.0 + v / 100.0).prod() - 1.0) * 100.0), "% acumulado"
    if unidade.startswith("%"):
        return float(v.iloc[-1] - v.iloc[0]), "p.p."
    return float((v.iloc[-1] / v.iloc[0] - 1.0) * 100.0), "%"

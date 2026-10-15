"""Motor de regras declarativas: severidade, dimensões e score.

Cada regra é uma `Regra` com uma função `verificar(df) -> (avaliavel, falha)`: duas séries
booleanas alinhadas ao índice do DataFrame. `falha` só é True onde `avaliavel` é True.
Regras cujas colunas não existem no DataFrame são marcadas como "ignorada", não como falha.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from .validadores import cnpj_valido, cpf_valido

DIMENSOES = ("completude", "unicidade", "validade", "consistencia", "atualidade")
SEVERIDADES = ("erro", "aviso")
EXEMPLOS_POR_REGRA = 5
Verificacao = Callable[[pd.DataFrame], tuple[pd.Series, pd.Series]]


@dataclass(frozen=True)
class Regra:
    id: str
    descricao: str
    dimensao: str
    severidade: str
    colunas: tuple[str, ...]
    verificar: Verificacao = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.dimensao not in DIMENSOES:
            raise ValueError(f"dimensão inválida em {self.id}: {self.dimensao}")
        if self.severidade not in SEVERIDADES:
            raise ValueError(f"severidade inválida em {self.id}: {self.severidade}")


# --- fábricas de regras -------------------------------------------------------------

def _mascara_aplicavel(df: pd.DataFrame, apenas: tuple[str, str] | None) -> pd.Series:
    if apenas is None:
        return pd.Series(True, index=df.index)
    coluna, valor = apenas
    return df[coluna].astype("string").eq(valor).fillna(False).astype(bool)


def _texto(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _numero(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _data(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format="ISO8601")


def completude(id: str, coluna: str, severidade: str, descricao: str = "") -> Regra:
    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        vazio = _texto(df[coluna]).isna() | _texto(df[coluna]).eq("")
        return pd.Series(True, index=df.index), vazio.fillna(True).astype(bool)

    return Regra(id, descricao or f"`{coluna}` preenchido", "completude", severidade, (coluna,), v)


def unicidade(id: str, coluna: str, severidade: str, descricao: str = "") -> Regra:
    """Marca todas as linhas que compartilham o valor duplicado (keep=False)."""

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        avaliavel = df[coluna].notna()
        return avaliavel, df[coluna].duplicated(keep=False) & avaliavel

    return Regra(id, descricao or f"`{coluna}` sem repetição", "unicidade", severidade, (coluna,), v)


def duplicatas_exatas(id: str, colunas: Iterable[str], severidade: str, descricao: str = "") -> Regra:
    """Marca as ocorrências extras de uma linha repetida (mantém a primeira)."""
    cols = tuple(colunas)

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        return pd.Series(True, index=df.index), df.duplicated(subset=list(cols), keep="first")

    return Regra(id, descricao or "linha não duplicada (" + ", ".join(cols) + ")", "unicidade", severidade, cols, v)


def regex(id: str, coluna: str, padrao: str, severidade: str, descricao: str = "") -> Regra:
    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        texto = _texto(df[coluna])
        avaliavel = texto.notna() & texto.ne("")
        ok = texto.str.fullmatch(padrao).fillna(False).astype(bool)
        return avaliavel, avaliavel & ~ok

    return Regra(id, descricao or f"`{coluna}` no formato esperado", "validade", severidade, (coluna,), v)


def _documento(id: str, coluna: str, validador: Callable[[object], bool], severidade: str,
               apenas: tuple[str, str] | None, descricao: str) -> Regra:
    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        avaliavel = df[coluna].notna() & _mascara_aplicavel(df, apenas)
        ok = df[coluna].map(validador).astype(bool)
        return avaliavel, avaliavel & ~ok

    return Regra(id, descricao, "validade", severidade, (coluna,), v)


def cnpj(id: str, coluna: str, severidade: str, apenas: tuple[str, str] | None = None, descricao: str = "") -> Regra:
    return _documento(id, coluna, cnpj_valido, severidade, apenas,
                      descricao or f"`{coluna}` é um CNPJ com dígito verificador válido")


def cpf(id: str, coluna: str, severidade: str, apenas: tuple[str, str] | None = None, descricao: str = "") -> Regra:
    return _documento(id, coluna, cpf_valido, severidade, apenas,
                      descricao or f"`{coluna}` é um CPF com dígito verificador válido")


def dominio(id: str, coluna: str, valores: Iterable[str], severidade: str, descricao: str = "",
            dimensao: str = "validade") -> Regra:
    """Valor pertence a um conjunto fechado (ex.: UF, tipo de pessoa)."""
    permitidos = set(valores)

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        texto = _texto(df[coluna])
        avaliavel = texto.notna() & texto.ne("")
        ok = texto.isin(permitidos).fillna(False).astype(bool)
        return avaliavel, avaliavel & ~ok

    return Regra(id, descricao or f"`{coluna}` dentro do domínio", dimensao, severidade, (coluna,), v)


def numerico(id: str, coluna: str, severidade: str, descricao: str = "") -> Regra:
    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        avaliavel = df[coluna].notna() & _texto(df[coluna]).ne("")
        return avaliavel, avaliavel & _numero(df[coluna]).isna()

    return Regra(id, descricao or f"`{coluna}` é numérico", "validade", severidade, (coluna,), v)


def data_valida(id: str, coluna: str, severidade: str, descricao: str = "") -> Regra:
    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        avaliavel = df[coluna].notna() & _texto(df[coluna]).ne("")
        return avaliavel, avaliavel & _data(df[coluna]).isna()

    return Regra(id, descricao or f"`{coluna}` é uma data válida (AAAA-MM-DD)", "validade", severidade,
                 (coluna,), v)


def nao_negativo(id: str, coluna: str, severidade: str, descricao: str = "") -> Regra:
    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        numero = _numero(df[coluna])
        avaliavel = numero.notna()
        return avaliavel, avaliavel & (numero < 0)

    return Regra(id, descricao or f"`{coluna}` não é negativo", "consistencia", severidade, (coluna,), v)


def ordem_datas(id: str, anterior: str, posterior: str, severidade: str, descricao: str = "") -> Regra:
    """Falha quando `posterior` é anterior a `anterior` (ex.: fim de vigência antes do início)."""

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        a, b = _data(df[anterior]), _data(df[posterior])
        avaliavel = a.notna() & b.notna()
        return avaliavel, avaliavel & (b < a)

    return Regra(id, descricao or f"`{posterior}` não é anterior a `{anterior}`", "consistencia", severidade,
                 (anterior, posterior), v)


def referencial(id: str, coluna: str, chave: str, mapa: dict[str, str], severidade: str,
                descricao: str, prefixo: int | None = None) -> Regra:
    """Confere `coluna` contra uma tabela de referência: `mapa[chave]` deve bater com a coluna.

    Com `prefixo=n`, compara só os n primeiros caracteres da coluna (ex.: UF <-> código IBGE).
    """

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        esperado = _texto(df[chave]).map(mapa)
        real = _texto(df[coluna])
        if prefixo is not None:
            real = real.str.slice(0, prefixo)
        avaliavel = esperado.notna() & real.notna()
        return avaliavel, avaliavel & real.ne(esperado)

    return Regra(id, descricao, "consistencia", severidade, (coluna, chave), v)


def outlier_iqr(id: str, coluna: str, severidade: str, k: float = 3.0, descricao: str = "") -> Regra:
    """Fora de [Q1 - k*IQR, Q3 + k*IQR] (k=3 é o critério de valor extremo)."""

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        x = _numero(df[coluna])
        avaliavel = x.notna()
        q1, q3 = x.quantile(0.25), x.quantile(0.75)
        amplitude = q3 - q1
        fora = (x < q1 - k * amplitude) | (x > q3 + k * amplitude)
        return avaliavel, avaliavel & fora.fillna(False)

    return Regra(id, descricao or f"`{coluna}` sem valor extremo (IQR, k={k:g})", "validade", severidade,
                 (coluna,), v)


def outlier_mad(id: str, coluna: str, severidade: str, limite: float = 3.5, log10: bool = False,
                descricao: str = "") -> Regra:
    """Escore z modificado por MAD (Iglewicz-Hoaglin): |0,6745*(x - mediana)/MAD| > limite.

    Com `log10=True` a regra roda na escala logarítmica (valores <= 0 não são avaliados),
    que evita que distribuições assimétricas, como valores de contrato, gerem falsos extremos.
    """

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        x = _numero(df[coluna])
        avaliavel = x.notna() & (x > 0) if log10 else x.notna()
        dados = np.log10(x.where(avaliavel)) if log10 else x.where(avaliavel)
        mediana = dados.median()
        mad = (dados - mediana).abs().median()
        if not mad or np.isnan(mad):
            return avaliavel, pd.Series(False, index=df.index)
        z = 0.6745 * (dados - mediana) / mad
        return avaliavel, avaliavel & (z.abs() > limite).fillna(False)

    escala = " (escala log10)" if log10 else ""
    return Regra(id, descricao or f"`{coluna}` sem valor extremo (MAD{escala}, limite={limite:g})",
                 "validade", severidade, (coluna,), v)


def atualidade(id: str, coluna: str, referencia: pd.Timestamp, dias_max: int, severidade: str,
               descricao: str = "") -> Regra:
    """Registro cuja data de atualização é mais antiga que `dias_max` em relação à data de referência."""

    def v(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        d = _data(df[coluna])
        avaliavel = d.notna()
        return avaliavel, avaliavel & ((referencia - d).dt.days > dias_max).fillna(False)

    return Regra(id, descricao or f"`{coluna}` atualizado nos últimos {dias_max} dias", "atualidade",
                 severidade, (coluna,), v)


# --- execução ----------------------------------------------------------------------

def _exemplos(df: pd.DataFrame, falha: pd.Series, colunas: tuple[str, ...],
              mascarar: Callable[[pd.DataFrame], pd.DataFrame] | None) -> list[dict[str, Any]]:
    amostra = df.loc[falha].head(EXEMPLOS_POR_REGRA)
    if mascarar is not None:
        amostra = mascarar(amostra)
    cols = [c for c in colunas if c in amostra.columns]
    amostra = amostra[cols]
    return [{k: (None if pd.isna(v) else v) for k, v in linha.items()} for linha in amostra.to_dict("records")]


def avaliar(df: pd.DataFrame, regras: Iterable[Regra],
            mascarar: Callable[[pd.DataFrame], pd.DataFrame] | None = None) -> dict[str, Any]:
    """Executa as regras e devolve resultados por regra e as máscaras de falha por linha.

    `mascarar` é aplicado aos exemplos de linhas que falharam (ex.: ocultar CPF antes de exibir).
    Retorno: {"regras": [...], "falhas_por_regra": {id: Series}, "linhas_com_erro": Series}.
    """
    resultados: list[dict[str, Any]] = []
    falhas_por_regra: dict[str, pd.Series] = {}
    erro_mask = pd.Series(False, index=df.index)
    for regra in regras:
        ausentes = [c for c in regra.colunas if c not in df.columns]
        base = {"id": regra.id, "descricao": regra.descricao, "dimensao": regra.dimensao,
                "severidade": regra.severidade, "colunas": list(regra.colunas)}
        if ausentes:
            resultados.append({**base, "status": "ignorada", "avaliadas": 0, "falhas": 0,
                               "pct_falha": None, "aprovacao": None, "exemplos": [],
                               "motivo": "colunas ausentes: " + ", ".join(ausentes)})
            continue
        avaliavel, falha = regra.verificar(df)
        avaliavel = avaliavel.astype(bool)
        falha = (falha.astype(bool) & avaliavel)
        n_aval, n_falha = int(avaliavel.sum()), int(falha.sum())
        pct = (n_falha / n_aval) if n_aval else 0.0
        resultados.append({**base, "status": "passou" if n_falha == 0 else "falhou", "avaliadas": n_aval,
                           "falhas": n_falha, "pct_falha": round(100 * pct, 2),
                           "aprovacao": round(1 - pct, 6) if n_aval else None,
                           "exemplos": _exemplos(df, falha, regra.colunas, mascarar), "motivo": None})
        falhas_por_regra[regra.id] = falha
        if regra.severidade == "erro":
            erro_mask |= falha
    return {"regras": resultados, "falhas_por_regra": falhas_por_regra, "linhas_com_erro": erro_mask}


def score_dimensoes(resultados: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Por dimensão: média das taxas de aprovação das regras avaliadas e contagem de regras que passaram."""
    saida: dict[str, dict[str, Any]] = {}
    for dim in DIMENSOES:
        dos = [r for r in resultados if r["dimensao"] == dim and r["status"] != "ignorada"]
        aprov = [r["aprovacao"] for r in dos if r["aprovacao"] is not None]
        saida[dim] = {
            "regras": len([r for r in resultados if r["dimensao"] == dim]),
            "regras_avaliadas": len(dos),
            "regras_passaram": sum(1 for r in dos if r["status"] == "passou"),
            "score_pct": round(100 * float(np.mean(aprov)), 2) if aprov else None,
        }
    return saida

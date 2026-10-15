"""Achata o JSON de contratos do PNCP em um DataFrame plano e mascara dados de pessoa física."""
from __future__ import annotations

from typing import Any

import pandas as pd

PF_MASCARA = "CPF oculto (pessoa física)"

# coluna plana -> caminho no JSON do PNCP
MAPA: dict[str, tuple[str, ...]] = {
    "numero_controle_pncp": ("numeroControlePNCP",),
    "numero_retificacao": ("numeroRetificacao",),
    "cnpj_orgao": ("orgaoEntidade", "cnpj"),
    "razao_social_orgao": ("orgaoEntidade", "razaoSocial"),
    "tipo_pessoa": ("tipoPessoa",),
    "ni_fornecedor": ("niFornecedor",),
    "nome_fornecedor": ("nomeRazaoSocialFornecedor",),
    "tipo_contrato": ("tipoContrato", "nome"),
    "categoria_processo": ("categoriaProcesso", "nome"),
    "objeto_contrato": ("objetoContrato",),
    "valor_inicial": ("valorInicial",),
    "valor_parcela": ("valorParcela",),
    "valor_global": ("valorGlobal",),
    "data_assinatura": ("dataAssinatura",),
    "data_vigencia_inicio": ("dataVigenciaInicio",),
    "data_vigencia_fim": ("dataVigenciaFim",),
    "data_publicacao": ("dataPublicacaoPncp",),
    "data_atualizacao": ("dataAtualizacao",),
    "uf_sigla": ("unidadeOrgao", "ufSigla"),
    "codigo_ibge": ("unidadeOrgao", "codigoIbge"),
}


def _caminho(registro: dict[str, Any], caminho: tuple[str, ...]) -> Any:
    atual: Any = registro
    for chave in caminho:
        if not isinstance(atual, dict):
            return None
        atual = atual.get(chave)
    return atual


def normalizar(registros: list[dict[str, Any]]) -> pd.DataFrame:
    """Uma linha por contrato, colunas em MAPA, tipos básicos (texto, número, data ISO)."""
    df = pd.DataFrame({col: [_caminho(r, cam) for r in registros] for col, cam in MAPA.items()})
    for col in ("valor_inicial", "valor_parcela", "valor_global"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["numero_retificacao"] = pd.to_numeric(df["numero_retificacao"], errors="coerce")
    for col in ("data_assinatura", "data_vigencia_inicio", "data_vigencia_fim", "data_publicacao", "data_atualizacao"):
        # a API mistura "AAAA-MM-DD" e "AAAA-MM-DDTHH:MM:SS"; guardamos só a data
        df[col] = df[col].astype("string").str.slice(0, 10)
    return df


def mascarar_pessoa_fisica(df: pd.DataFrame) -> pd.DataFrame:
    """Oculta CPF e nome do fornecedor em registros de pessoa física ou com número de formato CPF.

    Inclui registros marcados como PJ mas com 11 dígitos (formato de CPF), pois o nome seria pessoal.
    Usado antes de exportar e de exibir exemplos; a validação roda sobre os dados originais.
    """
    saida = df.copy()
    if "ni_fornecedor" not in saida.columns:
        return saida
    digitos = saida["ni_fornecedor"].astype("string").fillna("").str.replace(r"\D", "", regex=True)
    pf = digitos.str.len().eq(11).fillna(False).astype(bool)
    if "tipo_pessoa" in saida.columns:
        pf |= saida["tipo_pessoa"].astype("string").eq("PF").fillna(False).astype(bool)
    saida.loc[pf, "ni_fornecedor"] = PF_MASCARA
    if "nome_fornecedor" in saida.columns:
        saida.loc[pf, "nome_fornecedor"] = PF_MASCARA
    return saida

"""Catálogo de regras para contratos do PNCP (colunas do `normalizar.MAPA`)."""
from __future__ import annotations

import pandas as pd

from . import regras as R

UF_CODIGO_IBGE: dict[str, str] = {
    "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15", "AP": "16", "TO": "17",
    "MA": "21", "PI": "22", "CE": "23", "RN": "24", "PB": "25", "PE": "26", "AL": "27",
    "SE": "28", "BA": "29", "MG": "31", "ES": "32", "RJ": "33", "SP": "35", "PR": "41",
    "SC": "42", "RS": "43", "MS": "50", "MT": "51", "GO": "52", "DF": "53",
}
TIPOS_PESSOA = ("PF", "PJ", "PE")  # pessoa física, jurídica, estrangeiro
PADRAO_NUMERO_PNCP = r"\d{14}-\d+-\d+/\d{4}"
DIAS_ATUALIDADE = 730


def regras_contratos(data_referencia: pd.Timestamp) -> list[R.Regra]:
    """Regras de qualidade para a tabela normalizada de contratos.

    `data_referencia` é a data da extração (atualidade é medida contra ela, não contra "hoje").
    """
    return [
        # completude
        R.completude("C001", "ni_fornecedor", "erro"),
        R.completude("C002", "objeto_contrato", "aviso"),
        R.completude("C003", "valor_global", "erro"),
        R.completude("C004", "cnpj_orgao", "erro"),
        R.completude("C005", "data_publicacao", "erro"),
        # unicidade
        R.unicidade("U001", "numero_controle_pncp", "erro"),
        R.duplicatas_exatas("U002", ["numero_controle_pncp", "numero_retificacao", "data_atualizacao", "valor_global"], "erro"),
        # validade
        R.regex("V001", "numero_controle_pncp", PADRAO_NUMERO_PNCP, "erro"),
        R.cnpj("V002", "cnpj_orgao", "erro"),
        R.cnpj("V003", "ni_fornecedor", "erro", apenas=("tipo_pessoa", "PJ"),
               descricao="CNPJ do fornecedor (pessoa jurídica) com dígito verificador válido"),
        R.cpf("V004", "ni_fornecedor", "erro", apenas=("tipo_pessoa", "PF"),
              descricao="CPF do fornecedor (pessoa física) com dígito verificador válido"),
        R.dominio("V005", "tipo_pessoa", TIPOS_PESSOA, "erro", "tipo de pessoa do fornecedor no domínio PF/PJ/PE"),
        R.dominio("V006", "uf_sigla", UF_CODIGO_IBGE, "erro", "UF da unidade é uma das 27 UFs"),
        R.numerico("V007", "valor_global", "erro"),
        R.data_valida("V008", "data_assinatura", "erro"),
        R.data_valida("V009", "data_vigencia_inicio", "aviso"),
        R.data_valida("V010", "data_vigencia_fim", "aviso"),
        R.outlier_iqr("V011", "valor_global", "aviso", k=3.0),
        R.outlier_mad("V012", "valor_global", "aviso", limite=3.5, log10=True),
        # consistência
        R.nao_negativo("K001", "valor_global", "erro"),
        R.nao_negativo("K002", "valor_inicial", "erro"),
        R.ordem_datas("K003", "data_vigencia_inicio", "data_vigencia_fim", "erro",
                      "fim da vigência não é anterior ao início"),
        R.ordem_datas("K004", "data_assinatura", "data_publicacao", "aviso",
                      "publicação não é anterior à assinatura"),
        R.referencial("K005", "codigo_ibge", "uf_sigla", UF_CODIGO_IBGE, "erro",
                      "código IBGE do município pertence à UF informada (dois primeiros dígitos)", prefixo=2),
        # atualidade
        R.atualidade("A001", "data_atualizacao", data_referencia, DIAS_ATUALIDADE, "aviso",
                     f"registro atualizado em até {DIAS_ATUALIDADE} dias antes da extração"),
    ]

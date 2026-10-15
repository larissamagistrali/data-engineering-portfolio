"""Normalização do JSON do PNCP e mascaramento de dados de pessoa física."""
from data_quality.normalizar import PF_MASCARA, mascarar_pessoa_fisica, normalizar


def _registro(**extra):
    base = {
        "numeroControlePNCP": "00394460005887-2-007096/2024", "numeroRetificacao": 0,
        "orgaoEntidade": {"cnpj": "00394460005887", "razaoSocial": "MIN"}, "tipoPessoa": "PJ",
        "niFornecedor": "11222333000181", "nomeRazaoSocialFornecedor": "ACME",
        "tipoContrato": {"id": 1, "nome": "Contrato"}, "categoriaProcesso": {"id": 1, "nome": "Compra"},
        "objetoContrato": "Objeto", "valorInicial": 1, "valorParcela": 1, "valorGlobal": 1,
        "dataAssinatura": "2025-01-02", "dataVigenciaInicio": "2025-01-02T00:00:00",
        "dataVigenciaFim": "2025-02-02", "dataPublicacaoPncp": "2025-01-03T10:00:00",
        "dataAtualizacao": "2025-01-03T10:00:00", "unidadeOrgao": {"ufSigla": "PR", "codigoIbge": "4106902"},
    }
    base.update(extra)
    return base


def test_normalizar_achata_e_trunca_datas():
    df = normalizar([_registro()])
    assert df.loc[0, "cnpj_orgao"] == "00394460005887"
    assert df.loc[0, "uf_sigla"] == "PR"
    assert df.loc[0, "data_vigencia_inicio"] == "2025-01-02"
    assert df.loc[0, "valor_global"] == 1.0


def test_mascara_cpf_de_pessoa_fisica_e_de_pj_com_formato_cpf():
    df = normalizar([
        _registro(tipoPessoa="PF", niFornecedor="52998224725", nomeRazaoSocialFornecedor="FULANO"),
        _registro(tipoPessoa="PJ", niFornecedor="52998224725", nomeRazaoSocialFornecedor="BELTRANO"),
        _registro(),
    ])
    mascarado = mascarar_pessoa_fisica(df)
    assert mascarado["ni_fornecedor"].tolist()[:2] == [PF_MASCARA, PF_MASCARA]
    assert mascarado["nome_fornecedor"].tolist()[:2] == [PF_MASCARA, PF_MASCARA]
    assert mascarado.loc[2, "ni_fornecedor"] == "11222333000181"
    assert df.loc[0, "ni_fornecedor"] == "52998224725"  # original preservado

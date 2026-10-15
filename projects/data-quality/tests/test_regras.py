"""Regras declarativas: casos bons e ruins, outliers, duplicatas, datas, domínios e score."""
import numpy as np
import pandas as pd

from data_quality import regras as R
from data_quality.catalogo import UF_CODIGO_IBGE, regras_contratos


def _status(resultados, rid):
    return next(r for r in resultados if r["id"] == rid)


def _avaliar(df, *regras):
    return R.avaliar(df, regras)["regras"]


def test_completude_marca_nulos_e_vazios():
    df = pd.DataFrame({"a": ["x", None, "  ", "y"]})
    r = _avaliar(df, R.completude("C1", "a", "erro"))[0]
    assert (r["avaliadas"], r["falhas"], r["status"]) == (4, 2, "falhou")


def test_unicidade_marca_todas_as_ocorrencias():
    df = pd.DataFrame({"id": ["1", "2", "2", None]})
    r = _avaliar(df, R.unicidade("U1", "id", "erro"))[0]
    assert (r["avaliadas"], r["falhas"]) == (3, 2)


def test_duplicatas_exatas_mantem_a_primeira():
    df = pd.DataFrame({"k": [1, 1, 1, 2], "v": [5, 5, 6, 7]})
    r = _avaliar(df, R.duplicatas_exatas("D1", ["k", "v"], "erro"))[0]
    assert r["falhas"] == 1 and r["status"] == "falhou"


def test_regex_formato_do_numero_pncp():
    df = pd.DataFrame({"n": ["00394460005887-2-007096/2024", "123", None]})
    r = _avaliar(df, R.regex("V1", "n", r"\d{14}-\d+-\d+/\d{4}", "erro"))[0]
    assert (r["avaliadas"], r["falhas"]) == (2, 1)


def test_cnpj_com_condicao_so_avalia_pessoa_juridica():
    df = pd.DataFrame({"tipo": ["PJ", "PF", "PJ"], "doc": ["11.222.333/0001-81", "999", "11.222.333/0001-82"]})
    r = _avaliar(df, R.cnpj("V1", "doc", "erro", apenas=("tipo", "PJ")))[0]
    assert (r["avaliadas"], r["falhas"]) == (2, 1)


def test_cpf_com_condicao_so_avalia_pessoa_fisica():
    df = pd.DataFrame({"tipo": ["PF", "PF", "PJ"], "doc": ["529.982.247-25", "529.982.247-26", "x"]})
    r = _avaliar(df, R.cpf("V1", "doc", "erro", apenas=("tipo", "PF")))[0]
    assert (r["avaliadas"], r["falhas"]) == (2, 1)


def test_dominio_uf():
    df = pd.DataFrame({"uf": ["SP", "XX", None]})
    r = _avaliar(df, R.dominio("V2", "uf", UF_CODIGO_IBGE.keys(), "erro"))[0]
    assert (r["avaliadas"], r["falhas"]) == (2, 1)


def test_numerico_e_data_valida():
    df = pd.DataFrame({"v": ["10.5", "abc"], "d": ["2025-01-31", "31/01/2025"]})
    rn, rd = _avaliar(df, R.numerico("N", "v", "erro"), R.data_valida("D", "d", "erro"))
    assert rn["falhas"] == 1
    assert rd["falhas"] == 1


def test_nao_negativo():
    df = pd.DataFrame({"v": [0.0, 10.0, -1.0, np.nan]})
    r = _avaliar(df, R.nao_negativo("K", "v", "erro"))[0]
    assert (r["avaliadas"], r["falhas"]) == (3, 1)


def test_ordem_datas_fim_antes_do_inicio():
    df = pd.DataFrame({"ini": ["2025-01-01", "2025-03-01", None], "fim": ["2025-02-01", "2025-02-01", "2025-01-01"]})
    r = _avaliar(df, R.ordem_datas("K", "ini", "fim", "erro"))[0]
    assert (r["avaliadas"], r["falhas"]) == (2, 1)


def test_iqr_detecta_extremo_e_ignora_valores_normais():
    valores = list(np.linspace(100, 200, 50)) + [1_000_000.0]
    df = pd.DataFrame({"v": valores})
    r = _avaliar(df, R.outlier_iqr("O", "v", "aviso", k=3.0))[0]
    assert r["falhas"] == 1


def test_mad_na_escala_log_nao_marca_serie_regular():
    valores = [10.0 ** e for e in np.linspace(3, 6, 60)]
    df = pd.DataFrame({"v": valores + [1e12]})
    r = _avaliar(df, R.outlier_mad("O", "v", "aviso", log10=True))[0]
    assert r["falhas"] == 1
    assert r["exemplos"][0]["v"] == 1e12


def test_mad_com_mad_zero_nao_falha():
    df = pd.DataFrame({"v": [5.0] * 10})
    r = _avaliar(df, R.outlier_mad("O", "v", "aviso"))[0]
    assert r["falhas"] == 0


def test_referencial_uf_codigo_ibge_com_prefixo():
    df = pd.DataFrame({"uf": ["PR", "PR", "SP"], "ibge": ["4106902", "3550308", "3550308"]})
    r = _avaliar(df, R.referencial("K", "ibge", "uf", UF_CODIGO_IBGE, "erro", "x", prefixo=2))[0]
    assert (r["avaliadas"], r["falhas"]) == (3, 1)


def test_atualidade_compara_com_data_de_referencia():
    df = pd.DataFrame({"d": ["2024-01-01", "2026-06-01"]})
    r = _avaliar(df, R.atualidade("A", "d", pd.Timestamp("2026-10-14"), 730, "aviso"))[0]
    assert r["falhas"] == 1


def test_coluna_ausente_vira_ignorada_e_nao_falha():
    df = pd.DataFrame({"a": [1]})
    r = _avaliar(df, R.completude("C", "b", "erro"))[0]
    assert r["status"] == "ignorada" and r["falhas"] == 0 and "b" in r["motivo"]


def test_severidade_e_dimensao_invalidas_levantam_erro():
    import pytest

    with pytest.raises(ValueError):
        R.Regra("x", "x", "inexistente", "erro", (), lambda df: None)
    with pytest.raises(ValueError):
        R.Regra("x", "x", "validade", "critico", (), lambda df: None)


def test_avaliar_mascara_linhas_com_erro_e_score():
    df = pd.DataFrame({"a": ["x", None, "z"], "v": [1.0, -1.0, 2.0]})
    regras = [R.completude("C", "a", "erro"), R.nao_negativo("K", "v", "aviso")]
    res = R.avaliar(df, regras)
    assert res["linhas_com_erro"].tolist() == [False, True, False]
    scores = R.score_dimensoes(res["regras"])
    assert scores["completude"]["regras_passaram"] == 0
    assert scores["completude"]["score_pct"] == round(100 * (2 / 3), 2)


def test_catalogo_contratos_bom_e_ruim():
    """Um contrato bom passa nas regras de erro; um ruim falha exatamente nas esperadas."""
    ref = pd.Timestamp("2025-06-30")
    bom = {
        "numero_controle_pncp": "00394460005887-2-007096/2024", "numero_retificacao": 0,
        "cnpj_orgao": "00394460005887", "tipo_pessoa": "PJ", "ni_fornecedor": "11222333000181",
        "nome_fornecedor": "ACME", "objeto_contrato": "Serviço", "valor_inicial": 7000.0,
        "valor_parcela": 7000.0, "valor_global": 7000.0, "data_assinatura": "2025-01-02",
        "data_vigencia_inicio": "2025-01-02", "data_vigencia_fim": "2025-12-31",
        "data_publicacao": "2025-01-03", "data_atualizacao": "2025-01-03",
        "uf_sigla": "PR", "codigo_ibge": "4106902",
    }
    ruim = {**bom, "numero_controle_pncp": "lixo", "ni_fornecedor": "11222333000182",
            "data_vigencia_fim": "2024-12-31", "uf_sigla": "SP", "valor_global": -5.0,
            "objeto_contrato": None}
    sem_uf = {**bom, "uf_sigla": "XX", "numero_controle_pncp": "00394460005887-2-007097/2024"}
    df = pd.DataFrame([bom, ruim, sem_uf])
    res = R.avaliar(df, regras_contratos(ref))
    por_id = {r["id"]: r for r in res["regras"]}
    assert por_id["V003"]["falhas"] == 1
    assert por_id["V001"]["falhas"] == 1
    assert por_id["K003"]["falhas"] == 1
    assert por_id["K001"]["falhas"] == 1
    assert por_id["V006"]["falhas"] == 1  # só a UF "XX"
    assert por_id["K005"]["falhas"] == 1  # UF SP com código IBGE do Paraná
    assert por_id["C002"]["falhas"] == 1
    assert res["linhas_com_erro"].tolist() == [False, True, True]

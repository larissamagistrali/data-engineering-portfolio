from datetime import date

import pandas as pd

from bcb_ingestion.incremental import DEFAULT_START, last_date, next_window_start
from bcb_ingestion.validate import validate_observations


def _df(datas, valores, codigo=1):
    return pd.DataFrame({
        "data": pd.to_datetime(datas), "serie": "dolar_venda", "codigo": codigo,
        "valor": pd.Series(valores, dtype="float64"),
    })


def test_carga_incremental_comeca_na_ultima_data_inclusive():
    silver = _df(["2026-10-01", "2026-10-02"], [5.1, 5.2])
    assert last_date(silver) == date(2026, 10, 2)
    assert next_window_start(last_date(silver)) == date(2026, 10, 2)


def test_carga_inicial_usa_data_padrao_quando_nao_ha_silver():
    assert last_date(None) is None
    assert next_window_start(None) == DEFAULT_START


def test_validacao_aceita_serie_consistente():
    assert validate_observations(_df(["2024-01-01", "2024-01-02"], [1.0, 2.0]), today=date(2026, 1, 1)) == []


def test_validacao_detecta_nulos_duplicatas_e_futuro():
    df = _df(["2024-01-01", "2024-01-01", "2030-01-01"], [1.0, None, 2.0])
    problemas = validate_observations(df, today=date(2026, 1, 1))
    texto = " | ".join(problemas)
    assert "nulos" in texto and "duplicadas" in texto and "futuro" in texto


def test_validacao_detecta_serie_vazia_e_colunas_faltando():
    assert validate_observations(pd.DataFrame()) == ["série sem observações"]
    assert "colunas ausentes" in validate_observations(pd.DataFrame({"data": [1]}))[0]

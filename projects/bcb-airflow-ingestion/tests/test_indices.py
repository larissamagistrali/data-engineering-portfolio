import pandas as pd
import pytest

from bcb_ingestion.client import SERIES
from bcb_ingestion.indices import UNIDADES, nivel_comparavel, variacao_no_periodo


def test_toda_serie_coletada_tem_natureza_definida():
    assert set(SERIES.values()) == set(UNIDADES)


def test_variacao_mensal_vira_indice_acumulado():
    r = pd.Series([1.0, 2.0, -1.0])  # % a.m.
    nivel = nivel_comparavel(r, "% a.m.")
    assert (nivel / nivel.iloc[0] * 100).round(4).tolist() == [100.0, 102.0, 100.98]


def test_taxa_em_nivel_fica_fora_do_indice_e_nivel_passa_direto():
    s = pd.Series([10.0, 12.0])
    assert nivel_comparavel(s, "% a.a.") is None
    assert nivel_comparavel(s, "R$/US$").equals(s)


@pytest.mark.parametrize(
    ("valores", "unidade", "esperado"),
    [
        ([5.0, 5.5], "R$/US$", (10.0, "%")),
        ([1.0, 1.0], "% a.m.", (2.01, "% acumulado")),
        ([13.75, 15.0], "% a.a.", (1.25, "p.p.")),
    ],
)
def test_variacao_no_periodo_usa_a_metrica_de_cada_natureza(valores, unidade, esperado):
    valor, rotulo = variacao_no_periodo(pd.Series(valores), unidade)
    assert (round(valor, 4), rotulo) == esperado

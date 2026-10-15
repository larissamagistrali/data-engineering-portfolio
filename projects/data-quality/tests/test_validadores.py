"""CNPJ e CPF: casos válidos e inválidos conhecidos."""
import pytest

from data_quality.validadores import cnpj_valido, cpf_valido


@pytest.mark.parametrize("valor", ["11.222.333/0001-81", "00.000.000/0001-91", "11222333000181", "06.121.879/0001-06"])
def test_cnpj_valido(valor):
    assert cnpj_valido(valor)


@pytest.mark.parametrize("valor", ["11.222.333/0001-82", "00000000000000", "11111111111111", "1122233300018", "", None, "abc"])
def test_cnpj_invalido(valor):
    assert not cnpj_valido(valor)


@pytest.mark.parametrize("valor", ["529.982.247-25", "111.444.777-35", "12345678909", "52998224725"])
def test_cpf_valido(valor):
    assert cpf_valido(valor)


@pytest.mark.parametrize("valor", ["529.982.247-26", "00000000000", "11111111111", "1234567890", None, float("nan")])
def test_cpf_invalido(valor):
    assert not cpf_valido(valor)

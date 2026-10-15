"""Validadores de CNPJ e CPF pelo dígito verificador (somente formato numérico)."""
from __future__ import annotations

import re

_NAO_DIGITO = re.compile(r"\D")


def _digitos(valor: object) -> str:
    """Mantém só os dígitos; devolve '' para valores nulos."""
    if valor is None:
        return ""
    texto = str(valor).strip()
    if texto.lower() in ("", "nan", "none", "nat"):
        return ""
    return _NAO_DIGITO.sub("", texto)


def _dv(base: str, pesos: list[int]) -> int:
    soma = sum(int(d) * p for d, p in zip(base, pesos))
    resto = soma % 11
    return 0 if resto < 2 else 11 - resto


def cnpj_valido(valor: object) -> bool:
    """CNPJ com 14 dígitos e dois dígitos verificadores corretos. Aceita pontuação."""
    cnpj = _digitos(valor)
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    d1 = _dv(cnpj[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = _dv(cnpj[:13], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return cnpj[12:] == f"{d1}{d2}"


def cpf_valido(valor: object) -> bool:
    """CPF com 11 dígitos e dois dígitos verificadores corretos. Aceita pontuação."""
    cpf = _digitos(valor)
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    d1 = _dv(cpf[:9], list(range(10, 1, -1)))
    d2 = _dv(cpf[:10], list(range(11, 1, -1)))
    return cpf[9:] == f"{d1}{d2}"

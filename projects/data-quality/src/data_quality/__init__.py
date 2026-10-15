"""Validação de qualidade de dados em Python puro (DuckDB/pandas), sem Great Expectations.

Módulos: `validadores` (CNPJ/CPF por dígito verificador), `regras` (regras declarativas,
severidade, dimensões, score), `catalogo` (regras dos contratos do PNCP), `normalizar`
(achata o JSON do PNCP) e `pipeline` (coleta opcional, valida e exporta).
"""

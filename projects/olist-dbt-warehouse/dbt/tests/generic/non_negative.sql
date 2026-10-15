{# Teste genérico próprio: falha nas linhas em que a coluna é negativa. #}
{% test non_negative(model, column_name) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < 0
{% endtest %}

{# Unicidade de uma combinação de colunas (equivalente enxuto ao unique_combination_of_columns do dbt_utils, sem depender de pacote). #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

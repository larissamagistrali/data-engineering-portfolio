{# Teste genérico próprio: coluna de data não pode ser anterior a outra coluna (quando ambas existem). #}
{% test column_not_before(model, column_name, other_column) %}
select {{ column_name }}, {{ other_column }}
from {{ model }}
where {{ column_name }} is not null
  and {{ other_column }} is not null
  and {{ column_name }} < {{ other_column }}
{% endtest %}

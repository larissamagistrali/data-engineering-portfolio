-- Teste singular: coerência lógica entre is_late e delay_days.
select order_id, is_late, delay_days
from {{ ref('fct_orders') }}
where (is_late and delay_days <= 0)
   or (is_delivered and not is_late and delay_days > 0)

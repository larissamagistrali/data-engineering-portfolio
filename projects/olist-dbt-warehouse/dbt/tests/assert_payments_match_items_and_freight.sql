-- Teste singular: para pedidos válidos COM itens, o total pago deve bater (tolerância de R$ 0,05)
-- com itens + frete. Falha mostra os pedidos divergentes.
select order_id, order_value, payment_value
from {{ ref('fct_orders') }}
where is_valid_order
  and items_count > 0
  and (payment_value is null or abs(payment_value - order_value) > 0.05)

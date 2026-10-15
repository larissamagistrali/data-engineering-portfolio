-- Agrega os itens por pedido (grão: 1 linha por pedido que tem itens).
select
    order_id,
    count(*) as items_count,
    count(distinct product_id) as distinct_products,
    count(distinct seller_id) as distinct_sellers,
    sum(price) as items_value,
    sum(freight_value) as freight_value,
    sum(price + freight_value) as order_value
from {{ ref('stg_order_items') }}
group by order_id

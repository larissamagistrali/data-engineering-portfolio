-- Grão: 1 linha por item de pedido.
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    purchase_date,
    order_status,
    order_status not in ('canceled', 'unavailable') as is_valid_order,
    customer_unique_id,
    customer_state,
    seller_state,
    same_state,
    category_en,
    price,
    freight_value,
    item_total
from {{ ref('int_order_items_enriched') }}

-- Itens enriquecidos com contexto do pedido, produto, vendedor e cliente (base de fct_order_items).
select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    cast(o.purchased_at as date) as purchase_date,
    o.order_status,
    c.customer_unique_id,
    c.state as customer_state,
    s.state as seller_state,
    c.state = s.state as same_state,
    coalesce(t.category_name_en, p.category_name) as category_en,
    p.category_name as category_pt,
    i.price,
    i.freight_value,
    i.price + i.freight_value as item_total
from {{ ref('stg_order_items') }} as i
inner join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
inner join {{ ref('stg_customers') }} as c on c.customer_id = o.customer_id
inner join {{ ref('stg_sellers') }} as s on s.seller_id = i.seller_id
inner join {{ ref('stg_products') }} as p on p.product_id = i.product_id
left join {{ ref('stg_product_category_translation') }} as t on t.category_name = p.category_name

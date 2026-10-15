-- Grão: 1 linha por pedido. "Pedido válido" exclui cancelados e indisponíveis (usado nos KPIs de receita).
select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.state as customer_state,
    cast(o.purchased_at as date) as purchase_date,
    cast(date_trunc('month', o.purchased_at) as date) as purchase_month,
    o.order_status,
    o.order_status not in ('canceled', 'unavailable') as is_valid_order,
    coalesce(i.items_count, 0) as items_count,
    coalesce(i.items_value, 0) as items_value,
    coalesce(i.freight_value, 0) as freight_value,
    coalesce(i.order_value, 0) as order_value,
    p.payment_value,
    p.max_installments,
    p.main_payment_type,
    r.review_score,
    d.is_delivered,
    d.delivery_days,
    d.estimated_days,
    d.delay_days,
    d.is_late,
    d.delivery_status
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_customers') }} as c on c.customer_id = o.customer_id
left join {{ ref('int_order_items_agg') }} as i on i.order_id = o.order_id
left join {{ ref('int_order_payments_agg') }} as p on p.order_id = o.order_id
left join {{ ref('int_order_reviews_latest') }} as r on r.order_id = o.order_id
left join {{ ref('int_orders_delivery') }} as d on d.order_id = o.order_id

-- Ranking de vendedores por receita de itens (pedidos válidos), com qualidade (nota, atraso).
with seller_orders as (
    select
        i.seller_id,
        i.order_id,
        count(*) as items,
        sum(i.price) as revenue_items,
        sum(i.freight_value) as revenue_freight
    from {{ ref('fct_order_items') }} as i
    where i.is_valid_order
    group by i.seller_id, i.order_id
),

agg as (
    select
        so.seller_id,
        count(*) as orders,
        sum(so.items) as items,
        sum(so.revenue_items) as revenue_items,
        sum(so.revenue_freight) as revenue_freight,
        avg(o.review_score) as avg_review_score,
        count(*) filter (where o.is_delivered) as delivered_orders,
        count(*) filter (where o.is_late) as late_orders
    from seller_orders as so
    inner join {{ ref('fct_orders') }} as o using (order_id)
    group by so.seller_id
)

select
    rank() over (order by a.revenue_items desc) as revenue_rank,
    a.seller_id,
    s.state,
    s.city,
    s.region,
    a.orders,
    a.items,
    a.revenue_items,
    a.revenue_freight,
    round(a.avg_review_score, 3) as avg_review_score,
    a.delivered_orders,
    a.late_orders,
    round(100.0 * a.late_orders / nullif(a.delivered_orders, 0), 2) as pct_late
from agg as a
inner join {{ ref('dim_sellers') }} as s using (seller_id)
order by revenue_rank

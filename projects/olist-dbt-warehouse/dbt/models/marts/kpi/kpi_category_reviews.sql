-- Nota média por categoria (inglês). Um pedido com itens de 2 categorias conta uma vez em cada.
with order_category as (
    select distinct order_id, category_en
    from {{ ref('fct_order_items') }}
    where is_valid_order
),

reviews as (
    select
        oc.category_en,
        count(*) as orders,
        count(o.review_score) as reviewed_orders,
        avg(o.review_score) as avg_review_score,
        count(*) filter (where o.review_score <= 2) as low_score_orders,
        count(*) filter (where o.is_late) as late_orders
    from order_category as oc
    inner join {{ ref('fct_orders') }} as o using (order_id)
    group by oc.category_en
),

revenue as (
    select category_en, count(*) as items_sold, sum(price) as revenue_items
    from {{ ref('fct_order_items') }}
    where is_valid_order
    group by category_en
)

select
    r.category_en,
    v.items_sold,
    r.orders,
    r.reviewed_orders,
    round(r.avg_review_score, 3) as avg_review_score,
    r.low_score_orders,
    round(100.0 * r.low_score_orders / nullif(r.reviewed_orders, 0), 2) as pct_low_score,
    r.late_orders,
    v.revenue_items
from reviews as r
inner join revenue as v using (category_en)
order by v.revenue_items desc

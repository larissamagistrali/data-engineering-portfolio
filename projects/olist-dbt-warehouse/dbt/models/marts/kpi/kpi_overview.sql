-- Uma linha com os números-resumo do painel.
with valid as (
    select * from {{ ref('fct_orders') }} where is_valid_order
)

select
    (select count(*) from {{ ref('fct_orders') }}) as orders_total,
    (select count(*) from valid) as orders_valid,
    (select count(*) from {{ ref('dim_customers') }}) as customers,
    (select count(*) from {{ ref('dim_sellers') }}) as sellers,
    (select count(*) from {{ ref('dim_products') }}) as products,
    (select sum(order_value) from valid) as revenue_total,
    round((select sum(order_value) / nullif(count(*) filter (where items_count > 0), 0) from valid), 2) as avg_ticket,
    round(100.0 * (select count(*) filter (where is_late) from valid where is_delivered)
        / (select count(*) from valid where is_delivered), 2) as pct_late,
    round((select avg(delivery_days) from valid where is_delivered), 2) as avg_delivery_days,
    round((select avg(review_score) from valid), 3) as avg_review_score,
    round(100.0 * (select count(*) filter (where is_repeat_customer) from {{ ref('dim_customers') }})
        / (select count(*) from {{ ref('dim_customers') }}), 2) as repeat_customer_pct,
    (select min(purchase_date) from valid) as first_purchase_date,
    (select max(purchase_date) from valid) as last_purchase_date

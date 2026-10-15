-- Receita mensal (pedidos válidos): itens + frete, ticket médio e crescimento mês a mês.
with monthly as (
    select
        purchase_month as month,
        count(*) as orders,
        count(*) filter (where items_count > 0) as orders_with_items,
        count(distinct customer_unique_id) as customers,
        sum(items_value) as revenue_items,
        sum(freight_value) as revenue_freight,
        sum(order_value) as revenue_total
    from {{ ref('fct_orders') }}
    where is_valid_order
    group by purchase_month
)

select
    month,
    orders,
    orders_with_items,
    customers,
    revenue_items,
    revenue_freight,
    revenue_total,
    round(revenue_total / nullif(orders_with_items, 0), 2) as avg_ticket,
    round(100.0 * (revenue_total / nullif(lag(revenue_total) over (order by month), 0) - 1), 1) as mom_growth_pct
from monthly
order by month

-- Grão: customer_unique_id (o cliente real, não o customer_id por pedido).
with orders as (
    select
        c.customer_unique_id,
        c.state,
        c.city,
        c.zip_code_prefix,
        o.order_id,
        o.purchased_at,
        o.order_status,
        coalesce(a.order_value, 0) as order_value
    from {{ ref('stg_orders') }} as o
    inner join {{ ref('stg_customers') }} as c on c.customer_id = o.customer_id
    left join {{ ref('int_order_items_agg') }} as a on a.order_id = o.order_id
),

latest as (
    select customer_unique_id, state, city, zip_code_prefix
    from orders
    qualify row_number() over (partition by customer_unique_id order by purchased_at desc, order_id) = 1
),

summary as (
    select
        customer_unique_id,
        min(purchased_at) as first_order_at,
        max(purchased_at) as last_order_at,
        count(*) as orders_count,
        sum(order_value) filter (where order_status not in ('canceled', 'unavailable')) as lifetime_value
    from orders
    group by customer_unique_id
)

select
    s.customer_unique_id,
    l.state,
    l.city,
    l.zip_code_prefix,
    s.first_order_at,
    s.last_order_at,
    s.orders_count,
    coalesce(s.lifetime_value, 0) as lifetime_value,
    s.orders_count > 1 as is_repeat_customer
from summary as s
inner join latest as l using (customer_unique_id)

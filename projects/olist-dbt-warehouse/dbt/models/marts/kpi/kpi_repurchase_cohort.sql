-- Coorte de recompra: clientes agrupados pelo mês da 1a compra (pedidos válidos).
--   retention_rate: % da coorte que comprou de novo EXATAMENTE no mês +k.
--   cumulative_repurchase_rate: % da coorte que já tinha feito alguma recompra até o mês +k.
with orders as (
    select distinct customer_unique_id, purchase_month
    from {{ ref('fct_orders') }}
    where is_valid_order
),

first_order as (
    select customer_unique_id, min(purchase_month) as cohort_month
    from orders
    group by customer_unique_id
),

activity as (
    select
        f.customer_unique_id,
        f.cohort_month,
        date_diff('month', f.cohort_month, o.purchase_month) as month_offset
    from orders as o
    inner join first_order as f using (customer_unique_id)
),

first_repeat as (
    select customer_unique_id, cohort_month, min(month_offset) filter (where month_offset > 0) as first_repeat_offset
    from activity
    group by customer_unique_id, cohort_month
),

cohorts as (
    select cohort_month, count(*) as cohort_size from first_order group by cohort_month
),

horizon as (
    select max(purchase_month) as last_month from orders
),

grid as (
    select
        c.cohort_month,
        c.cohort_size,
        k.month_offset
    from cohorts as c
    cross join horizon as h
    cross join lateral (
        select unnest(generate_series(0, date_diff('month', c.cohort_month, h.last_month))) as month_offset
    ) as k
)

select
    g.cohort_month,
    g.month_offset,
    g.cohort_size,
    (select count(*) from activity as a
        where a.cohort_month = g.cohort_month and a.month_offset = g.month_offset) as active_customers,
    (select count(*) from first_repeat as r
        where r.cohort_month = g.cohort_month and r.first_repeat_offset <= g.month_offset) as repurchased_customers,
    round(100.0 * (select count(*) from activity as a
        where a.cohort_month = g.cohort_month and a.month_offset = g.month_offset) / g.cohort_size, 3) as retention_rate,
    round(100.0 * (select count(*) from first_repeat as r
        where r.cohort_month = g.cohort_month and r.first_repeat_offset <= g.month_offset) / g.cohort_size, 3) as cumulative_repurchase_rate
from grid as g
order by g.cohort_month, g.month_offset

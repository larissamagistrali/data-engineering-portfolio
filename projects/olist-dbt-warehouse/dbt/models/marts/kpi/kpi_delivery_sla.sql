-- SLA de entrega por mês de compra e UF do cliente. Guarda somas e contagens para permitir
-- reagregar sem distorcer médias (média das médias é erro comum).
select
    purchase_month as month,
    customer_state,
    count(*) as delivered_orders,
    count(*) filter (where is_late) as late_orders,
    sum(delivery_days) as delivery_days_sum,
    sum(estimated_days) as estimated_days_sum,
    coalesce(sum(delay_days) filter (where is_late), 0) as delay_days_sum_late,
    round(100.0 * count(*) filter (where is_late) / count(*), 2) as pct_late,
    round(avg(delivery_days), 2) as avg_delivery_days
from {{ ref('fct_orders') }}
where is_delivered and delivery_days is not null
group by purchase_month, customer_state

-- Teste singular de reconciliação: a soma da receita mensal (gold) deve igualar a soma em fct_orders.
with kpi as (select sum(revenue_total) as total from {{ ref('kpi_monthly_revenue') }}),
fact as (select sum(order_value) as total from {{ ref('fct_orders') }} where is_valid_order)
select kpi.total as kpi_total, fact.total as fact_total
from kpi, fact
where abs(kpi.total - fact.total) > 0.01

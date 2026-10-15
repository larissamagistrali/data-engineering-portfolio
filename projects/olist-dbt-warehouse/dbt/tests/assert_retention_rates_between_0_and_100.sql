-- Teste singular: taxas de recompra são percentuais válidos e o acumulado nunca decresce no tempo.
with c as (
    select
        cohort_month,
        month_offset,
        retention_rate,
        cumulative_repurchase_rate,
        lag(cumulative_repurchase_rate) over (partition by cohort_month order by month_offset) as prev_cum
    from {{ ref('kpi_repurchase_cohort') }}
)

select *
from c
where retention_rate not between 0 and 100
   or cumulative_repurchase_rate not between 0 and 100
   or cumulative_repurchase_rate < prev_cum

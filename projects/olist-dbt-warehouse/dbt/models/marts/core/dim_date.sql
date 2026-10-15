-- Dimensão de calendário gerada a partir do intervalo de compras (sem dbt_utils).
with bounds as (
    select
        min(cast(purchased_at as date)) as first_day,
        max(cast(purchased_at as date)) as last_day
    from {{ ref('stg_orders') }}
),

days as (
    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as date_day
    from bounds
)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    strftime(date_day, '%Y-%m') as year_month,
    cast(date_trunc('month', date_day) as date) as month_start,
    isodow(date_day) as iso_day_of_week,
    case isodow(date_day)
        when 1 then 'segunda' when 2 then 'terça' when 3 then 'quarta' when 4 then 'quinta'
        when 5 then 'sexta' when 6 then 'sábado' else 'domingo'
    end as day_name_pt,
    weekofyear(date_day) as week_of_year,
    isodow(date_day) >= 6 as is_weekend
from days

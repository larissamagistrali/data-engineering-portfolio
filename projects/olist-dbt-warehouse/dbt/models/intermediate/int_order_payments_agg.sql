-- Consolida pagamentos por pedido; a forma "principal" é a de maior valor.
with ranked as (
    select
        *,
        row_number() over (partition by order_id order by payment_value desc, payment_sequential) as rn
    from {{ ref('stg_order_payments') }}
)

select
    order_id,
    sum(payment_value) as payment_value,
    count(*) as payments_count,
    max(payment_installments) as max_installments,
    max(case when rn = 1 then payment_type end) as main_payment_type,
    bool_or(payment_type = 'voucher') as used_voucher
from ranked
group by order_id

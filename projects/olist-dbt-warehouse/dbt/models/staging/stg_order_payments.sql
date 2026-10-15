select
    order_id,
    cast(payment_sequential as integer) as payment_sequential,
    lower(trim(payment_type)) as payment_type,
    cast(payment_installments as integer) as payment_installments,
    cast(payment_value as decimal(12, 2)) as payment_value
from {{ source('olist', 'order_payments') }}

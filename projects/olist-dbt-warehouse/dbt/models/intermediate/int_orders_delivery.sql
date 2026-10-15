-- Métricas logísticas por pedido. "Atrasado" = entregue em data posterior à estimativa prometida.
select
    order_id,
    order_status,
    purchased_at,
    delivered_at,
    estimated_delivery_at,
    order_status = 'delivered' and delivered_at is not null as is_delivered,
    date_diff('hour', purchased_at, approved_at) as approval_hours,
    date_diff('day', purchased_at, delivered_at) as delivery_days,
    date_diff('day', purchased_at, estimated_delivery_at) as estimated_days,
    case
        when delivered_at is not null
            then date_diff('day', cast(estimated_delivery_at as date), cast(delivered_at as date))
    end as delay_days,
    coalesce(cast(delivered_at as date) > cast(estimated_delivery_at as date), false) as is_late,
    case
        when delivered_at is null then 'nao_entregue'
        when cast(delivered_at as date) > cast(estimated_delivery_at as date) then 'atrasado'
        else 'no_prazo'
    end as delivery_status
from {{ ref('stg_orders') }}

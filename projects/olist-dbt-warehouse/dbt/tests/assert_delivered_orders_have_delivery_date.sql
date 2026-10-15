{{ config(severity='warn') }}
-- Teste singular com severidade "warn": pedidos 'delivered' sem data de entrega ao cliente.
-- O Olist real tem esse defeito em poucos pedidos; aqui ele é replicado de propósito no gerador
-- sintético para exercitar um aviso (não é erro de pipeline).
select order_id
from {{ ref('stg_orders') }}
where order_status = 'delivered' and delivered_at is null

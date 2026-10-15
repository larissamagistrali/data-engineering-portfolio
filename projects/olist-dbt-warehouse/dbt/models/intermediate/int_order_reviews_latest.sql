-- Um pedido pode ter mais de uma avaliação: ficamos com a mais recente (grão: 1 linha por pedido avaliado).
select
    order_id,
    review_id,
    review_score,
    comment_message is not null as has_comment,
    review_created_at
from {{ ref('stg_order_reviews') }}
qualify row_number() over (partition by order_id order by review_created_at desc, review_id) = 1

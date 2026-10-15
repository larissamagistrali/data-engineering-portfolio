select
    review_id,
    order_id,
    cast(review_score as integer) as review_score,
    nullif(trim(review_comment_title), '') as comment_title,
    nullif(trim(review_comment_message), '') as comment_message,
    try_cast(review_creation_date as timestamp) as review_created_at,
    try_cast(review_answer_timestamp as timestamp) as review_answered_at
from {{ source('olist', 'order_reviews') }}

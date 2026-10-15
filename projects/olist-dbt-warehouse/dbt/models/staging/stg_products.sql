-- Corrige o typo do dado original (product_name_lenght) e trata categoria ausente.
select
    product_id,
    coalesce(nullif(trim(product_category_name), ''), 'sem_categoria') as category_name,
    try_cast(product_name_lenght as integer) as name_length,
    try_cast(product_description_lenght as integer) as description_length,
    try_cast(product_photos_qty as integer) as photos_qty,
    try_cast(product_weight_g as double) as weight_g,
    try_cast(product_length_cm as double) as length_cm,
    try_cast(product_height_cm as double) as height_cm,
    try_cast(product_width_cm as double) as width_cm
from {{ source('olist', 'products') }}

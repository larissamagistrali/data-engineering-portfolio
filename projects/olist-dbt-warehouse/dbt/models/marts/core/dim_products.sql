select
    p.product_id,
    p.category_name as category_pt,
    coalesce(t.category_name_en, p.category_name) as category_en,
    p.category_name <> 'sem_categoria' as has_category,
    p.name_length,
    p.description_length,
    p.photos_qty,
    p.weight_g,
    p.length_cm * p.height_cm * p.width_cm as volume_cm3,
    case
        when p.weight_g is null then 'desconhecido'
        when p.weight_g < 500 then 'leve'
        when p.weight_g < 5000 then 'medio'
        else 'pesado'
    end as weight_class
from {{ ref('stg_products') }} as p
left join {{ ref('stg_product_category_translation') }} as t on t.category_name = p.category_name

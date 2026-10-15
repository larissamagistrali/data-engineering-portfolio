select
    seller_id,
    zip_code_prefix,
    city,
    state,
    case
        when state in ('SP', 'RJ', 'MG', 'ES') then 'Sudeste'
        when state in ('PR', 'SC', 'RS') then 'Sul'
        when state in ('DF', 'GO', 'MT', 'MS') then 'Centro-Oeste'
        when state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'Nordeste'
        else 'Norte'
    end as region
from {{ ref('stg_sellers') }}

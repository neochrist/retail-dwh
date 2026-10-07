select
    transaction_id,
    store_location
from {{ ref('stg_sales') }}
where store_id = ''

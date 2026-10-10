select
    year,
    month,
    store_id,
    count() as row_count
from {{ ref('dim_store_lfl') }}
group by
    year,
    month,
    store_id
having count() > 1

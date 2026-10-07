select
    sale_date,
    store_key,
    product_key,
    count() as row_count
from {{ ref('agg_sales_daily') }}
group by
    sale_date,
    store_key,
    product_key
having count() > 1

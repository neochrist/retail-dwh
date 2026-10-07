{{ config(order_by=['sale_date', 'store_key', 'product_key']) }}

select
    transaction_id,
    store_key,
    product_key,
    customer_id,
    sold_at,
    sale_date,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    revenue,
    discount_pct,
    category,
    payment_method,
    store_location
from {{ ref('stg_sales') }}

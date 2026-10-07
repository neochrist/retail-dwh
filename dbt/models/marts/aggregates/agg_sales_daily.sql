{{ config(order_by=['sale_date', 'store_key', 'product_key']) }}

select
    sale_date,
    store_key,
    product_key,
    sum(quantity) as quantity_total,
    sum(revenue) as revenue_total,
    sum(gross_amount) as gross_amount_total,
    sum(discount_amount) as discount_amount_total,
    if(
        gross_amount_total = 0,
        0,
        toFloat64(discount_amount_total) / toFloat64(gross_amount_total) * 100
    ) as weighted_discount_pct,
    count() as transaction_count,
    uniqExact(customer_id) as unique_customers
from {{ ref('fact_sales') }}
group by
    sale_date,
    store_key,
    product_key

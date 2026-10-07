with source as (
    select
        cityHash64(tuple(
            customer_id,
            product_id,
            quantity,
            price,
            transaction_date,
            payment_method,
            store_location,
            product_category,
            discount_pct,
            total_amount
        )) as transaction_id,
        customer_id,
        product_id,
        quantity,
        price,
        transaction_date,
        payment_method,
        store_location,
        product_category,
        discount_pct,
        total_amount
    from {{ source('raw', 'raw_sales') }}
),

typed as (
    select
        transaction_id,
        parseDateTimeInJodaSyntax(transaction_date, 'M/d/yyyy H:mm', 'Asia/Tbilisi') as sold_at,
        extract(store_location, '([A-Z]{2}) \\d{5}$') as store_id,
        product_id,
        customer_id,
        toUInt32(quantity) as quantity,
        toDecimal64(price, 9) as unit_price,
        toDecimal64(discount_pct, 9) as discount_pct,
        toDecimal64(total_amount, 9) as revenue,
        product_category as category,
        payment_method,
        store_location
    from source
)

select
    transaction_id,
    cityHash64(store_id) as store_key,
    cityHash64(product_id) as product_key,
    store_id,
    product_id,
    customer_id,
    sold_at,
    toDate(sold_at) as sale_date,
    quantity,
    unit_price,
    quantity * unit_price as gross_amount,
    gross_amount - revenue as discount_amount,
    revenue,
    discount_pct,
    category,
    payment_method,
    store_location
from typed

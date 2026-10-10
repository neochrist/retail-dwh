{{ config(order_by=['product_id']) }}

with products as (
    select distinct
        product_key,
        product_id
    from {{ ref('stg_sales') }}
),

name_parts as (
    select
        ['Classic', 'Premium', 'Everyday', 'Organic', 'Golden', 'Fresh'] as adjectives,
        ['Crunch', 'Blend', 'Harvest', 'Select', 'Choice'] as nouns
),

named_products as (
    select
        p.product_key,
        p.product_id,
        concat(
            n.adjectives[p.product_key % length(n.adjectives) + 1],
            ' ',
            n.nouns[intDiv(p.product_key, length(n.adjectives)) % length(n.nouns) + 1],
            ' ',
            p.product_id
        ) as product_name
    from products as p
    cross join name_parts as n
)

select
    np.product_key,
    np.product_id,
    np.product_name,
    a.abc_quantity,
    a.abc_amount
from named_products as np
left join {{ ref('product_abc') }} as a
    on a.product_key = np.product_key

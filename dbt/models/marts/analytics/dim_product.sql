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
)

select
    p.product_key as product_key,
    p.product_id as product_id,
    concat(
        n.adjectives[p.product_key % length(n.adjectives) + 1],
        ' ',
        n.nouns[intDiv(p.product_key, length(n.adjectives)) % length(n.nouns) + 1],
        ' ',
        p.product_id
    ) as product_name,
    a.abc_quantity,
    a.abc_amount
from products as p
cross join name_parts as n
left join {{ ref('product_abc') }} as a
    on a.product_key = p.product_key

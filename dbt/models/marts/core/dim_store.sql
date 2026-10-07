{{ config(order_by=['store_id']) }}

with stores as (
    select distinct
        store_key,
        store_id
    from {{ ref('stg_sales') }}
),

name_parts as (
    select
        ['Sunny', 'Green', 'Golden', 'Fresh', 'Corner', 'Liberty', 'Harbor', 'Maple'] as adjectives,
        ['Market', 'Grocer', 'Mart', 'Pantry', 'Basket', 'Store'] as nouns
)

select
    s.store_key,
    s.store_id,
    concat(
        n.adjectives[s.store_key % length(n.adjectives) + 1],
        ' ',
        n.nouns[intDiv(s.store_key, length(n.adjectives)) % length(n.nouns) + 1],
        ' ',
        s.store_id
    ) as store_name
from stores as s
cross join name_parts as n

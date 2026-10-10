{{ config(order_by=['product_id']) }}

with product_totals as (
    select
        product_key,
        sum(quantity_total) as total_quantity,
        sum(revenue_total) as total_revenue
    from {{ ref('agg_sales_daily') }}
    group by product_key
),

products as (
    select distinct
        product_key,
        product_id
    from {{ ref('stg_sales') }}
),

named_totals as (
    select
        t.product_key,
        p.product_id,
        t.total_quantity,
        t.total_revenue
    from product_totals as t
    inner join products as p
        on t.product_key = p.product_key
),

running_totals as (
    select
        product_key,
        product_id,
        total_quantity,
        sum(total_quantity) over () as grand_quantity,
        sum(total_quantity) over (
            order by total_quantity desc, product_id
            rows between unbounded preceding and current row
        ) as running_quantity,
        total_revenue,
        sum(total_revenue) over () as grand_revenue,
        sum(total_revenue) over (
            order by total_revenue desc, product_id
            rows between unbounded preceding and current row
        ) as running_revenue
    from named_totals
)

select
    product_key,
    product_id,
    total_quantity,
    toFloat64(total_quantity) / toFloat64(grand_quantity) * 100 as quantity_share_pct,
    toFloat64(running_quantity) / toFloat64(grand_quantity) * 100 as quantity_cumulative_pct,
    multiIf(
        running_quantity * 100 <= grand_quantity * 50, 'A',
        running_quantity * 100 <= grand_quantity * 70, 'B',
        'C'
    ) as abc_quantity,
    total_revenue,
    toFloat64(total_revenue) / toFloat64(grand_revenue) * 100 as revenue_share_pct,
    toFloat64(running_revenue) / toFloat64(grand_revenue) * 100 as revenue_cumulative_pct,
    multiIf(
        running_revenue * 100 <= grand_revenue * 50, 'A',
        running_revenue * 100 <= grand_revenue * 70, 'B',
        'C'
    ) as abc_amount
from running_totals

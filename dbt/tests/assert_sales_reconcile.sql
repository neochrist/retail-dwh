with raw_totals as (
    select
        count() as row_count,
        sum(toUInt32(quantity)) as quantity_sum,
        sum(toDecimal64(total_amount, 9)) as revenue_sum
    from {{ source('raw', 'raw_sales') }}
),

fact_totals as (
    select
        count() as row_count,
        sum(quantity) as quantity_sum,
        sum(revenue) as revenue_sum
    from {{ ref('fact_sales') }}
),

agg_totals as (
    select
        sum(transaction_count) as row_count,
        sum(quantity_total) as quantity_sum,
        sum(revenue_total) as revenue_sum
    from {{ ref('agg_sales_daily') }}
)

select
    r.row_count as raw_rows,
    f.row_count as fact_rows,
    a.row_count as agg_rows,
    r.quantity_sum as raw_quantity,
    f.quantity_sum as fact_quantity,
    a.quantity_sum as agg_quantity,
    r.revenue_sum as raw_revenue,
    f.revenue_sum as fact_revenue,
    a.revenue_sum as agg_revenue
from raw_totals as r
cross join fact_totals as f
cross join agg_totals as a
where r.row_count = 0
    or r.row_count != f.row_count
    or f.row_count != a.row_count
    or r.quantity_sum != f.quantity_sum
    or f.quantity_sum != a.quantity_sum
    or r.revenue_sum != f.revenue_sum
    or f.revenue_sum != a.revenue_sum

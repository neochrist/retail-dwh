{{ config(order_by=['year', 'month', 'store_id']) }}

with bounds as (
    select
        max(sale_date) as max_date,
        toYear(max_date) as current_year,
        toMonth(max_date) as last_month,
        toDayOfMonth(max_date) as last_day
    from {{ ref('agg_sales_daily') }}
),

months as (
    select
        toUInt8(number + 1) as month
    from numbers(12) as n
    cross join bounds
    where month <= last_month
),

grid as (
    select
        s.store_key,
        s.store_id,
        m.month
    from {{ ref('dim_store') }} as s
    cross join months as m
),

worked_days as (
    select
        store_key,
        toYear(sale_date) as sale_year,
        toMonth(sale_date) as sale_month,
        uniqExact(sale_date) as days_worked
    from {{ ref('agg_sales_daily') }}
    group by
        store_key,
        sale_year,
        sale_month
),

required_days as (
    select
        m.month,
        toDayOfMonth(toLastDayOfMonth(makeDate(b.current_year - 1, m.month, 1))) as days_required_py,
        if(
            m.month = b.last_month,
            b.last_day,
            toDayOfMonth(toLastDayOfMonth(makeDate(b.current_year, m.month, 1)))
        ) as days_required_cy
    from months as m
    cross join bounds as b
)

select
    b.current_year as year,
    g.month as month,
    g.store_id,
    if(
        cy.days_worked = r.days_required_cy
            and py.days_worked = r.days_required_py,
        1,
        0
    ) as lfl_status
from grid as g
cross join bounds as b
inner join required_days as r
    on r.month = g.month
left join worked_days as cy
    on cy.store_key = g.store_key
    and cy.sale_month = g.month
    and cy.sale_year = b.current_year
left join worked_days as py
    on py.store_key = g.store_key
    and py.sale_month = g.month
    and py.sale_year = b.current_year - 1

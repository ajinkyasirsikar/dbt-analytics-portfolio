-- Monthly category performance for trend analysis and seasonal planning
-- Grain: one row per year-month per category
-- Includes MoM growth and running totals for executive dashboards

with monthly as (
    select
        txn_year,
        txn_month,
        category,
        count(distinct txn_id) as transactions,
        count(distinct customer_id) as unique_customers,
        sum(quantity) as units_sold,
        sum(net_revenue) as net_revenue,
        sum(gross_profit) as gross_profit,
        round(avg(discount_pct), 1) as avg_discount_pct,
        count(distinct case when channel = 'online' then txn_id end) as online_transactions,
        count(distinct case when channel = 'store' then txn_id end) as store_transactions
    from {{ ref('fct_beauty_sales') }}
    group by 1, 2, 3
),

with_growth as (
    select
        *,
        -- month-over-month revenue growth
        round(
            (net_revenue - lag(net_revenue) over (
                partition by category order by txn_year, txn_month
            )) * 100.0 / nullif(lag(net_revenue) over (
                partition by category order by txn_year, txn_month
            ), 0),
            1
        ) as revenue_mom_growth_pct,

        -- online share
        round(online_transactions * 100.0 / nullif(transactions, 0), 1) as online_share_pct,

        -- cumulative revenue
        sum(net_revenue) over (
            partition by category order by txn_year, txn_month
        ) as cumulative_revenue
    from monthly
)

select * from with_growth
order by category, txn_year, txn_month

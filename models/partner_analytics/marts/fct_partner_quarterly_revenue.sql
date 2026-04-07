-- Quarterly revenue by partner, region, and product line
-- Designed for multi-region executive reporting and QoQ trend analysis
-- Grain: one row per partner × quarter × product line

with revenue as (
    select * from {{ ref('stg_partner_revenue') }}
),

partners as (
    select partner_id, partner_name, partner_tier, region_group
    from {{ ref('stg_partners') }}
),

quarterly as (
    select
        r.partner_id,
        p.partner_name,
        p.partner_tier,
        p.region_group,
        r.revenue_year,
        r.revenue_quarter,
        r.product_line,

        sum(r.amount_usd) as quarterly_revenue,
        sum(case when r.is_recurring then r.amount_usd else 0 end) as quarterly_recurring,
        count(*) as transaction_count
    from revenue r
    inner join partners p on r.partner_id = p.partner_id
    group by 1, 2, 3, 4, 5, 6, 7
),

with_growth as (
    select
        *,
        -- QoQ growth
        round(
            (quarterly_revenue - lag(quarterly_revenue) over (
                partition by partner_id, product_line
                order by revenue_year, revenue_quarter
            )) * 100.0 / nullif(lag(quarterly_revenue) over (
                partition by partner_id, product_line
                order by revenue_year, revenue_quarter
            ), 0),
            1
        ) as qoq_growth_pct
    from quarterly
)

select * from with_growth
order by partner_id, revenue_year, revenue_quarter, product_line

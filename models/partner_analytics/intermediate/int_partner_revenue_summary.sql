-- Intermediate: partner-level revenue aggregation
-- Used by both dim_partners and mart_region_summary
-- Prevents duplicating complex revenue logic across two marts

with revenue as (
    select * from {{ ref('stg_partner_revenue') }}
),

summary as (
    select
        partner_id,
        sum(amount_usd) as total_revenue,
        sum(case when is_recurring then amount_usd else 0 end) as recurring_revenue,
        sum(case when not is_recurring then amount_usd else 0 end) as services_revenue,
        count(distinct product_line) as product_lines_used,
        min(revenue_date) as first_revenue_date,
        max(revenue_date) as last_revenue_date,
        count(distinct revenue_year || '-' || revenue_quarter) as active_quarters,

        -- recent quarter revenue for trend detection
        sum(case
            when revenue_date >= current_date - interval '90 days'
            then amount_usd else 0
        end) as last_90d_revenue,

        -- revenue concentration: % from largest product line
        max(product_line_revenue) as largest_product_line_revenue
    from (
        select
            *,
            sum(amount_usd) over (partition by partner_id, product_line) as product_line_revenue
        from revenue
    )
    group by partner_id
)

select
    *,
    case
        when total_revenue > 0
        then round(recurring_revenue * 100.0 / total_revenue, 1)
        else 0
    end as recurring_pct,
    case
        when total_revenue > 0
        then round(largest_product_line_revenue * 100.0 / total_revenue, 1)
        else 0
    end as revenue_concentration_pct
from summary

-- Partner dimension with revenue and engagement health metrics
-- Mirrors the unified partner view from a multi-region data model

with partners as (
    select * from {{ ref('stg_partners') }}
),

revenue as (
    select
        partner_id,
        sum(amount_usd) as total_revenue,
        sum(case when is_recurring then amount_usd else 0 end) as recurring_revenue,
        sum(case when not is_recurring then amount_usd else 0 end) as services_revenue,
        count(distinct product_line) as product_lines_used,
        min(revenue_date) as first_revenue_date,
        max(revenue_date) as last_revenue_date,
        count(distinct revenue_year || '-' || revenue_quarter) as active_quarters
    from {{ ref('stg_partner_revenue') }}
    group by 1
),

engagement as (
    select
        partner_id,
        sum(case when event_type = 'api_calls' then metric_value else 0 end) as total_api_calls,
        sum(case when event_type = 'data_processed' then metric_value else 0 end) as total_tb_processed,
        count(distinct event_month || '-' || event_year) as active_engagement_months
    from {{ ref('stg_partner_engagement') }}
    group by 1
),

final as (
    select
        p.partner_id,
        p.partner_name,
        p.partner_tier,
        p.region,
        p.region_group,
        p.country_code,
        p.industry,
        p.onboarded_date,
        p.account_manager,
        p.is_active,
        p.tenure_days,

        -- revenue metrics
        coalesce(r.total_revenue, 0) as total_revenue,
        coalesce(r.recurring_revenue, 0) as recurring_revenue,
        coalesce(r.services_revenue, 0) as services_revenue,
        case
            when r.total_revenue > 0
            then round(r.recurring_revenue * 100.0 / r.total_revenue, 1)
            else 0
        end as recurring_revenue_pct,
        coalesce(r.product_lines_used, 0) as product_lines_used,
        r.first_revenue_date,
        r.last_revenue_date,
        coalesce(r.active_quarters, 0) as active_quarters,

        -- engagement metrics
        coalesce(e.total_api_calls, 0) as total_api_calls,
        coalesce(e.total_tb_processed, 0) as total_tb_processed,
        coalesce(e.active_engagement_months, 0) as active_engagement_months,

        -- health score: composite of revenue consistency + engagement depth + product adoption
        round(
            (case when r.active_quarters >= 4 then 30 when r.active_quarters >= 2 then 20 else 10 end) +
            (case when r.product_lines_used >= 3 then 30 when r.product_lines_used >= 2 then 20 else 10 end) +
            (case when e.total_api_calls >= 500000 then 40 when e.total_api_calls >= 100000 then 25 else 10 end)
        , 0) as health_score,

        -- revenue per day of tenure (efficiency)
        case
            when p.tenure_days > 0
            then round(coalesce(r.total_revenue, 0) / p.tenure_days, 2)
            else 0
        end as revenue_per_day

    from partners p
    left join revenue r on p.partner_id = r.partner_id
    left join engagement e on p.partner_id = e.partner_id
)

select * from final

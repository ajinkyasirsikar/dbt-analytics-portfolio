-- Partner dimension with revenue, engagement, and health metrics
-- Uses intermediate revenue summary for DRY aggregation

with partners as (
    select * from {{ ref('stg_partners') }}
),

revenue as (
    select * from {{ ref('int_partner_revenue_summary') }}
),

engagement as (
    select
        partner_id,
        sum(case when event_type = 'api_calls' then metric_value else 0 end) as total_api_calls,
        sum(case when event_type = 'data_processed' then metric_value else 0 end) as total_tb_processed,
        count(distinct event_month || '-' || event_year) as active_engagement_months,

        -- recent engagement trend
        sum(case
            when event_date >= current_date - interval '90 days' and event_type = 'api_calls'
            then metric_value else 0
        end) as last_90d_api_calls
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

        -- revenue (from intermediate)
        coalesce(r.total_revenue, 0) as total_revenue,
        coalesce(r.recurring_revenue, 0) as recurring_revenue,
        coalesce(r.services_revenue, 0) as services_revenue,
        coalesce(r.recurring_pct, 0) as recurring_revenue_pct,
        coalesce(r.revenue_concentration_pct, 0) as revenue_concentration_pct,
        coalesce(r.product_lines_used, 0) as product_lines_used,
        r.first_revenue_date,
        r.last_revenue_date,
        coalesce(r.active_quarters, 0) as active_quarters,
        coalesce(r.last_90d_revenue, 0) as last_90d_revenue,

        -- engagement
        coalesce(e.total_api_calls, 0) as total_api_calls,
        coalesce(e.total_tb_processed, 0) as total_tb_processed,
        coalesce(e.active_engagement_months, 0) as active_engagement_months,
        coalesce(e.last_90d_api_calls, 0) as last_90d_api_calls,

        -- health score: composite of revenue consistency + engagement depth + product adoption
        round(
            (case when r.active_quarters >= 4 then 30 when r.active_quarters >= 2 then 20 else 10 end) +
            (case when r.product_lines_used >= 3 then 30 when r.product_lines_used >= 2 then 20 else 10 end) +
            (case when e.total_api_calls >= 500000 then 40 when e.total_api_calls >= 100000 then 25 else 10 end)
        , 0) as health_score,

        -- revenue efficiency
        case
            when p.tenure_days > 0
            then round(coalesce(r.total_revenue, 0) / p.tenure_days, 2)
            else 0
        end as revenue_per_day,

        -- churn risk signals
        case
            when not p.is_active then 'churned'
            when e.last_90d_api_calls = 0 and r.last_90d_revenue = 0 then 'high_risk'
            when e.last_90d_api_calls = 0 or r.last_90d_revenue = 0 then 'medium_risk'
            when r.revenue_concentration_pct > 80 then 'concentration_risk'
            else 'healthy'
        end as risk_status

    from partners p
    left join revenue r on p.partner_id = r.partner_id
    left join engagement e on p.partner_id = e.partner_id
)

select * from final

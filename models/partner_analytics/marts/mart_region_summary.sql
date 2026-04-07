-- Regional summary: unified view across all partner metrics per region
-- This mirrors the cross-region reporting pattern from a multi-region UDM

with partners as (
    select * from {{ ref('dim_partners') }}
),

regional as (
    select
        region_group,

        -- partner counts
        count(*) as total_partners,
        count(case when is_active then 1 end) as active_partners,
        count(case when partner_tier = 'premier' then 1 end) as premier_partners,
        count(case when partner_tier = 'advanced' then 1 end) as advanced_partners,
        count(case when partner_tier = 'standard' then 1 end) as standard_partners,

        -- revenue
        sum(total_revenue) as total_revenue,
        sum(recurring_revenue) as recurring_revenue,
        round(avg(total_revenue), 2) as avg_revenue_per_partner,
        round(avg(case when partner_tier = 'premier' then total_revenue end), 2) as avg_premier_revenue,

        -- engagement
        sum(total_api_calls) as total_api_calls,
        sum(total_tb_processed) as total_tb_processed,
        round(avg(health_score), 1) as avg_health_score,

        -- product adoption
        round(avg(product_lines_used), 1) as avg_product_lines,

        -- at-risk: low health score among active partners
        count(case when is_active and health_score < 50 then 1 end) as at_risk_partners

    from partners
    group by 1
)

select * from regional
order by total_revenue desc

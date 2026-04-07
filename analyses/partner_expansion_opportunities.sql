-- ANALYSIS: Which partners have the highest expansion potential?
--
-- Business question: "Which active partners are using fewer product lines
-- than their peers in the same tier, and have strong engagement signals?"
--
-- This identifies upsell opportunities: partners who are engaged (high API usage)
-- but haven't adopted all available product lines yet.

with partners as (
    select * from {{ ref('dim_partners') }}
    where is_active
),

tier_benchmarks as (
    select
        partner_tier,
        round(avg(product_lines_used), 1) as avg_product_lines,
        round(avg(total_revenue), 2) as avg_revenue,
        round(avg(total_api_calls), 0) as avg_api_calls
    from partners
    group by 1
),

expansion_candidates as (
    select
        p.partner_id,
        p.partner_name,
        p.partner_tier,
        p.region_group,
        p.product_lines_used,
        t.avg_product_lines as tier_avg_product_lines,
        p.total_revenue,
        p.total_api_calls,
        p.health_score,
        p.account_manager,

        -- gap: how many more product lines could they adopt?
        round(t.avg_product_lines - p.product_lines_used, 1) as product_line_gap,

        -- engagement relative to tier average
        round(p.total_api_calls * 100.0 / nullif(t.avg_api_calls, 0), 1) as engagement_vs_tier_pct

    from partners p
    inner join tier_benchmarks t on p.partner_tier = t.partner_tier
    where p.product_lines_used < t.avg_product_lines  -- below tier average
      and p.health_score >= 50                          -- but healthy
)

select * from expansion_candidates
order by engagement_vs_tier_pct desc

-- INSIGHT: Partners with high engagement_vs_tier_pct but low product_lines_used
-- are the best expansion targets: they're actively using the platform but haven't
-- discovered all it can do. Route these to account managers for outreach.

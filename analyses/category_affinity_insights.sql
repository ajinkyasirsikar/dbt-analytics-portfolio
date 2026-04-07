-- ANALYSIS: Which beauty categories drive cross-sell revenue?
--
-- Business question: "A customer who buys Skincare — what else do they buy,
-- and how much incremental revenue does that cross-sell generate?"
--
-- This is the type of analysis that informed category merchandising decisions
-- at a beauty retailer: which categories to co-locate, which bundles to offer,
-- and where to invest marketing spend for cross-category acquisition.

with cross_sell as (
    select * from {{ ref('mart_category_cross_sell') }}
),

ranked as (
    select
        category_a,
        category_b,
        shared_customers,
        avg_combined_spend,
        pct_a_also_buys_b,
        pct_b_also_buys_a,

        -- strongest affinity direction
        case
            when pct_a_also_buys_b > pct_b_also_buys_a
            then category_a || ' → ' || category_b
            else category_b || ' → ' || category_a
        end as strongest_direction,

        greatest(pct_a_also_buys_b, pct_b_also_buys_a) as max_affinity_pct

    from cross_sell
)

select * from ranked
order by max_affinity_pct desc

-- INSIGHT: Use this to identify:
-- 1. Which category pairs have the highest cross-purchase rate
-- 2. The "anchor" category (the one that drives the cross-sell)
-- 3. Average combined spend to size the bundle opportunity

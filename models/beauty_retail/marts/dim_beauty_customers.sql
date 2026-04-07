-- Customer dimension with lifetime metrics, category affinity, and behavioral segmentation
-- Uses intermediate models for DRY aggregation logic

with customers as (
    select * from {{ ref('stg_beauty_customers') }}
),

purchase_summary as (
    select * from {{ ref('int_customer_purchase_summary') }}
),

category_spend as (
    select * from {{ ref('int_customer_category_spend') }}
),

-- top category by revenue per customer
top_category as (
    select
        customer_id,
        category as top_category_by_spend,
        category_revenue as top_category_revenue
    from (
        select
            *,
            row_number() over (partition by customer_id order by category_revenue desc) as rn
        from category_spend
    )
    where rn = 1
),

-- cross-category sessions (bought from 2+ categories on same date)
cross_sell as (
    select
        customer_id,
        count(*) as cross_category_sessions
    from (
        select customer_id, txn_date, count(distinct category) as cats
        from {{ ref('fct_beauty_sales') }}
        group by 1, 2
        having count(distinct category) >= 2
    )
    group by 1
),

final as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.signup_date,
        c.loyalty_tier,
        c.preferred_category,
        c.metro_area,

        -- purchase metrics (from intermediate)
        coalesce(ps.total_transactions, 0) as total_transactions,
        coalesce(ps.categories_purchased, 0) as categories_purchased,
        coalesce(ps.unique_products, 0) as unique_products,
        coalesce(ps.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(ps.lifetime_profit, 0) as lifetime_profit,
        coalesce(ps.avg_transaction_value, 0) as avg_transaction_value,
        ps.first_purchase_date,
        ps.last_purchase_date,
        ps.active_months,
        ps.preferred_channel,
        coalesce(ps.holiday_revenue, 0) as holiday_revenue,
        coalesce(ps.avg_discount_used, 0) as avg_discount_used,
        coalesce(ps.discounted_purchases, 0) as discounted_purchases,

        -- channel mix
        coalesce(ps.online_purchases, 0) as online_purchases,
        coalesce(ps.store_purchases, 0) as store_purchases,
        case
            when ps.total_transactions > 0
            then round(ps.online_purchases * 100.0 / ps.total_transactions, 1)
            else 0
        end as online_pct,

        -- category affinity
        tc.top_category_by_spend,
        tc.top_category_revenue,

        -- behavioral flags
        case when ps.total_transactions > 1 then true else false end as is_repeat_buyer,
        coalesce(cs.cross_category_sessions, 0) as cross_category_sessions,
        case when cs.cross_category_sessions > 0 then true else false end as is_cross_category_buyer,

        -- discount sensitivity segment
        case
            when ps.avg_discount_used >= 10 then 'discount_driven'
            when ps.avg_discount_used > 0 then 'occasional_discount'
            else 'full_price'
        end as discount_segment,

        -- days to first purchase from signup
        case
            when ps.first_purchase_date is not null
            then ps.first_purchase_date - c.signup_date
        end as days_to_first_purchase,

        -- lifecycle status
        case
            when ps.total_transactions is null then 'never_purchased'
            when ps.last_purchase_date >= current_date - interval '90 days' then 'active'
            when ps.last_purchase_date >= current_date - interval '180 days' then 'at_risk'
            else 'lapsed'
        end as lifecycle_status,

        -- customer value tier (based on lifetime revenue quartiles)
        case
            when ps.lifetime_revenue >= 300 then 'high_value'
            when ps.lifetime_revenue >= 150 then 'mid_value'
            when ps.lifetime_revenue > 0 then 'low_value'
            else 'no_purchases'
        end as value_tier

    from customers c
    left join purchase_summary ps on c.customer_id = ps.customer_id
    left join top_category tc on c.customer_id = tc.customer_id
    left join cross_sell cs on c.customer_id = cs.customer_id
)

select * from final

-- Customer dimension with lifetime metrics, category affinity, and repurchase behavior
-- This is the kind of model that powered category-level decisions at Sephora

with customers as (
    select * from {{ ref('stg_beauty_customers') }}
),

sales as (
    select * from {{ ref('fct_beauty_sales') }}
),

customer_metrics as (
    select
        customer_id,
        count(distinct txn_id) as total_transactions,
        count(distinct category) as categories_purchased,
        sum(net_revenue) as lifetime_revenue,
        sum(gross_profit) as lifetime_profit,
        round(avg(net_revenue), 2) as avg_transaction_value,
        min(txn_date) as first_purchase_date,
        max(txn_date) as last_purchase_date,
        count(distinct txn_month || '-' || txn_year) as active_months,

        -- channel preference
        mode() within group (order by channel) as preferred_channel,

        -- top category by spend
        (select s2.category
         from {{ ref('fct_beauty_sales') }} s2
         where s2.customer_id = sales.customer_id
         group by s2.category
         order by sum(s2.net_revenue) desc
         limit 1
        ) as top_category_by_spend
    from sales
    group by customer_id
),

-- cross-category buyers (bought from 2+ categories in same transaction date)
cross_sell as (
    select
        customer_id,
        count(distinct txn_date) as cross_category_sessions
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

        coalesce(m.total_transactions, 0) as total_transactions,
        coalesce(m.categories_purchased, 0) as categories_purchased,
        coalesce(m.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(m.lifetime_profit, 0) as lifetime_profit,
        coalesce(m.avg_transaction_value, 0) as avg_transaction_value,
        m.first_purchase_date,
        m.last_purchase_date,
        m.active_months,
        m.preferred_channel,
        m.top_category_by_spend,

        -- repurchase: bought same category more than once
        case when m.total_transactions > 1 then true else false end as is_repeat_buyer,

        -- cross-category engagement
        coalesce(cs.cross_category_sessions, 0) as cross_category_sessions,
        case when cs.cross_category_sessions > 0 then true else false end as is_cross_category_buyer,

        -- days to first purchase from signup
        case
            when m.first_purchase_date is not null
            then m.first_purchase_date - c.signup_date
        end as days_to_first_purchase,

        -- lifecycle status
        case
            when m.total_transactions is null then 'never_purchased'
            when m.last_purchase_date >= current_date - interval '90 days' then 'active'
            when m.last_purchase_date >= current_date - interval '180 days' then 'at_risk'
            else 'lapsed'
        end as lifecycle_status

    from customers c
    left join customer_metrics m on c.customer_id = m.customer_id
    left join cross_sell cs on c.customer_id = cs.customer_id
)

select * from final

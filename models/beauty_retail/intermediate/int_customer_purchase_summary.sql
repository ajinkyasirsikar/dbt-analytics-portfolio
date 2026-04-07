-- Intermediate: customer-level purchase aggregation
-- Used by both dim_beauty_customers AND mart_category_cross_sell
-- Extracting this avoids duplicating the same aggregation logic in two marts

with sales as (
    select * from {{ ref('fct_beauty_sales') }}
),

customer_summary as (
    select
        customer_id,
        count(distinct txn_id) as total_transactions,
        count(distinct category) as categories_purchased,
        count(distinct product_id) as unique_products,
        sum(net_revenue) as lifetime_revenue,
        sum(gross_profit) as lifetime_profit,
        round(avg(net_revenue), 2) as avg_transaction_value,
        min(txn_date) as first_purchase_date,
        max(txn_date) as last_purchase_date,
        count(distinct txn_year || '-' || txn_month) as active_months,

        -- channel behavior
        count(case when channel = 'online' then 1 end) as online_purchases,
        count(case when channel = 'store' then 1 end) as store_purchases,
        mode() within group (order by channel) as preferred_channel,

        -- holiday behavior
        sum(case when is_holiday_season then net_revenue else 0 end) as holiday_revenue,

        -- discount sensitivity
        round(avg(discount_pct), 1) as avg_discount_used,
        count(case when discount_pct > 0 then 1 end) as discounted_purchases

    from sales
    group by customer_id
)

select * from customer_summary

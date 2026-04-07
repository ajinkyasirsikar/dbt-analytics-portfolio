-- Intermediate: customer × category spend matrix
-- Powers cross-sell analysis and category affinity calculations
-- Separated from marts so both cross_sell and customer dimension can reference it

with sales as (
    select * from {{ ref('fct_beauty_sales') }}
),

customer_category as (
    select
        customer_id,
        category,
        count(distinct txn_id) as category_transactions,
        sum(net_revenue) as category_revenue,
        sum(quantity) as category_units,
        min(txn_date) as first_category_purchase,
        max(txn_date) as last_category_purchase,

        -- repurchase: bought this category more than once
        case when count(distinct txn_date) > 1 then true else false end as is_repeat_in_category
    from sales
    group by 1, 2
)

select * from customer_category

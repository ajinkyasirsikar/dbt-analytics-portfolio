-- Category cross-sell analysis: which categories are bought together?
-- Answers: "A customer who buys Skincare — what else do they buy?"
-- This type of analysis directly informed category strategy at Sephora

with customer_categories as (
    select
        customer_id,
        category,
        sum(net_revenue) as category_revenue
    from {{ ref('fct_beauty_sales') }}
    group by 1, 2
),

-- self-join to find category pairs per customer
category_pairs as (
    select
        a.category as category_a,
        b.category as category_b,
        count(distinct a.customer_id) as shared_customers,
        round(avg(a.category_revenue + b.category_revenue), 2) as avg_combined_spend
    from customer_categories a
    inner join customer_categories b
        on a.customer_id = b.customer_id
        and a.category < b.category  -- avoid duplicates and self-pairs
    group by 1, 2
),

-- total customers per category for affinity %
category_totals as (
    select
        category,
        count(distinct customer_id) as total_customers
    from customer_categories
    group by 1
),

final as (
    select
        cp.category_a,
        cp.category_b,
        cp.shared_customers,
        cp.avg_combined_spend,
        ct_a.total_customers as category_a_total_customers,
        ct_b.total_customers as category_b_total_customers,

        -- affinity: what % of category A buyers also buy category B
        round(cp.shared_customers * 100.0 / ct_a.total_customers, 1) as pct_a_also_buys_b,
        round(cp.shared_customers * 100.0 / ct_b.total_customers, 1) as pct_b_also_buys_a
    from category_pairs cp
    left join category_totals ct_a on cp.category_a = ct_a.category
    left join category_totals ct_b on cp.category_b = ct_b.category
)

select * from final
order by shared_customers desc

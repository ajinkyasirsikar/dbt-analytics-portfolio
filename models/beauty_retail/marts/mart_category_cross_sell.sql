-- Category cross-sell analysis: which categories are bought together?
-- Uses intermediate customer_category_spend for DRY joins
-- Answers: "A customer who buys Skincare — what else do they buy?"

with customer_categories as (
    select * from {{ ref('int_customer_category_spend') }}
),

-- self-join to find category pairs per customer
category_pairs as (
    select
        a.category as category_a,
        b.category as category_b,
        count(distinct a.customer_id) as shared_customers,
        round(avg(a.category_revenue + b.category_revenue), 2) as avg_combined_spend,
        -- how many of these shared customers are repeat buyers in BOTH categories
        count(distinct case when a.is_repeat_in_category and b.is_repeat_in_category
              then a.customer_id end) as loyal_shared_customers
    from customer_categories a
    inner join customer_categories b
        on a.customer_id = b.customer_id
        and a.category < b.category
    group by 1, 2
),

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
        cp.loyal_shared_customers,
        ct_a.total_customers as category_a_total_customers,
        ct_b.total_customers as category_b_total_customers,

        -- affinity: what % of category A buyers also buy category B
        round(cp.shared_customers * 100.0 / ct_a.total_customers, 1) as pct_a_also_buys_b,
        round(cp.shared_customers * 100.0 / ct_b.total_customers, 1) as pct_b_also_buys_a,

        -- loyalty strength: what % of shared customers are loyal in both
        case
            when cp.shared_customers > 0
            then round(cp.loyal_shared_customers * 100.0 / cp.shared_customers, 1)
            else 0
        end as pct_loyal_shared,

        -- opportunity score: high affinity + high combined spend = bundle opportunity
        round(
            (cp.shared_customers * 1.0 / least(ct_a.total_customers, ct_b.total_customers)) *
            cp.avg_combined_spend / 100,
            2
        ) as bundle_opportunity_score
    from category_pairs cp
    left join category_totals ct_a on cp.category_a = ct_a.category
    left join category_totals ct_b on cp.category_b = ct_b.category
)

select * from final
order by bundle_opportunity_score desc

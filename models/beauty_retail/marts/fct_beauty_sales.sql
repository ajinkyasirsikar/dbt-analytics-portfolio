-- Fact table: enriched transactions at line-item grain
-- Joins product attributes for category/margin analysis

with txns as (
    select * from {{ ref('stg_beauty_transactions') }}
),

products as (
    select * from {{ ref('stg_beauty_products') }}
),

enriched as (
    select
        t.txn_id,
        t.customer_id,
        t.product_id,
        t.txn_date,
        t.txn_year,
        t.txn_quarter,
        t.txn_month,
        t.channel,
        t.store_id,
        t.quantity,
        t.unit_price,
        t.discount_pct,
        t.gross_revenue,
        t.net_revenue,

        -- product context
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.price_tier,
        p.gross_margin_pct,

        -- profit
        round(t.net_revenue * p.gross_margin_pct / 100, 2) as gross_profit
    from txns t
    inner join products p on t.product_id = p.product_id
)

select * from enriched

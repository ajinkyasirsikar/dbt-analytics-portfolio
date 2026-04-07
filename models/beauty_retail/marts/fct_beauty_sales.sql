-- Fact table: enriched transactions at line-item grain
-- INCREMENTAL: only processes new transactions since last run
-- In production, this is critical for tables with millions of daily rows
-- The is_incremental() pattern prevents full-table rescans

{{
    config(
        materialized='incremental',
        unique_key='txn_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with txns as (
    select * from {{ ref('stg_beauty_transactions') }}
    {% if is_incremental() %}
        where txn_date > (select max(txn_date) from {{ this }})
    {% endif %}
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
        round(t.net_revenue * p.gross_margin_pct / 100, 2) as gross_profit,

        -- time-based flags for analysis
        case
            when t.txn_month in (11, 12) then true
            else false
        end as is_holiday_season,

        case
            when t.txn_month in (3, 4, 5) then 'spring'
            when t.txn_month in (6, 7, 8) then 'summer'
            when t.txn_month in (9, 10, 11) then 'fall'
            else 'winter'
        end as season
    from txns t
    inner join products p on t.product_id = p.product_id
)

select * from enriched

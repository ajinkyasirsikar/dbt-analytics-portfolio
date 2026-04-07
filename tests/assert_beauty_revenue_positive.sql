-- Business rule: no completed beauty transaction should have zero or negative net revenue
-- Catches discount calculation bugs or data ingestion errors

select
    txn_id,
    net_revenue,
    discount_pct,
    gross_revenue
from {{ ref('fct_beauty_sales') }}
where net_revenue <= 0

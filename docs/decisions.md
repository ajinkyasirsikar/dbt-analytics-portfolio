# Modeling Decisions

## Why two domains?

Real analytics engineers rarely work with one data source. I chose two domains from my work history to show adaptability:

- **Beauty Retail** — B2C product analytics (category performance, cross-sell, customer lifecycle). Based on my experience leading analytics across Haircare, Fragrance, and Skincare categories.
- **Partner Channel** — B2B platform analytics (multi-region revenue, engagement health scoring, expansion opportunities). Based on my experience building a multi-region unified data model.

## Why star schema over One Big Table (OBT)?

OBT is simpler but collapses dimensionality. For these use cases:
- **Cross-sell analysis** requires joining customers across multiple product categories — a denormalized table would explode in width.
- **Partner health scoring** combines revenue + engagement + product adoption. Keeping dimensions separate lets each metric evolve independently without breaking downstream consumers.
- Star schema also enables self-serve analytics: business users can query `dim_customers` without needing to understand the full fact table.

## Why incremental materialization for fct_beauty_sales?

In production, transaction tables grow by millions of rows daily. Full-table rescans are expensive and slow. The `is_incremental()` pattern:
1. Only processes rows newer than the last run's max date
2. Uses `merge` strategy to handle late-arriving data
3. Supports `on_schema_change='append_new_columns'` for forward-compatible schema evolution

I chose this for the beauty sales fact specifically because it's the highest-volume table — the same pattern I'd use in production at scale.

## Why intermediate models?

Two marts needed the same aggregation:
- `dim_beauty_customers` needs customer-level purchase summaries
- `mart_category_cross_sell` needs customer × category spend

Without an intermediate layer, both marts would duplicate the same join and aggregation logic. `int_customer_purchase_summary` and `int_customer_category_spend` extract this shared logic once.

The intermediate models are `ephemeral` — they compile into CTEs in the downstream models rather than creating separate tables. This keeps the warehouse clean while maintaining code DRY-ness.

## Why snapshots?

`snap_customer_loyalty` tracks loyalty tier changes over time (SCD Type 2). Without this:
- You can't answer: "Was this customer gold or silver when they made this purchase?"
- Cohort analysis on loyalty programs becomes impossible
- You lose the ability to measure tier upgrade/downgrade impact on spending

Same logic applies to `snap_partner_tier` — understanding when a partner was upgraded to premier is essential for measuring the ROI of tier advancement.

## Data quality philosophy

Tests are organized at three levels, mirroring how I built production data quality monitoring:

1. **Source tests** — validate raw data contracts (uniqueness, referential integrity, accepted values). Catches ingestion problems before they propagate.
2. **Schema tests** — validate transformation logic (positive revenue, score bounds, valid enum values). Catches bugs in model logic.
3. **Custom generic tests** — reusable across domains:
   - `metric_not_negative`: any numeric column
   - `metric_variance`: anomaly detection when a metric deviates >X% from rolling average

The goal: catch issues at the earliest possible layer, not after they've propagated to dashboards. Every test is a contract that should hold true in production.

## Health score design (partner analytics)

The composite score (30-100) weights three signals:
- **Revenue consistency** (30 pts max): Are they paying regularly?
- **Product adoption** (30 pts max): Are they using multiple product lines?
- **Engagement depth** (40 pts max): Are they actually using the platform?

Engagement is weighted highest because API usage is the best leading indicator of churn — revenue is a lagging indicator. A partner can be paying but not using; that's a retention risk.

## Churn risk classification

`dim_partners.risk_status` uses a cascade of signals:
- `churned`: already inactive
- `high_risk`: zero recent revenue AND zero recent API calls
- `medium_risk`: zero in one but not both (partial disengagement)
- `concentration_risk`: >80% revenue from one product line (fragile)
- `healthy`: none of the above

This classification powers the partner health dashboard exposure and feeds into the expansion opportunity analysis.

## Exposures

I defined three downstream consumers to show how the data models connect to business tools:
- **Beauty category dashboard** — executive reporting on trends and cross-sell
- **Partner health report** — regional health tracking and churn risk
- **Customer segmentation export** — ML feature pipeline for personalization

Exposures are often overlooked in dbt projects, but they're critical for understanding the full data lineage from source to business impact.

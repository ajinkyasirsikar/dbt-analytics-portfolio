# Multi-Domain Analytics — dbt Portfolio

A production-style dbt project modeling two distinct business domains: **Beauty Retail** (B2C product analytics) and **Partner Channel** (B2B platform analytics). Demonstrates dimensional modeling, data quality testing, and CI/CD across different data contexts.

## Architecture

```
seeds (raw CSVs)
  → staging (cleaned, typed, standardized)
    → marts (star schema: facts + dimensions + analytical models)
```

### Beauty Retail Domain
Models inspired by category analytics across Haircare, Fragrance, and Skincare.

| Model | Type | What it answers |
|-------|------|-----------------|
| `fct_beauty_sales` | Fact | Enriched transactions with product/category context |
| `dim_beauty_customers` | Dimension | Lifetime value, repurchase behavior, cross-category affinity |
| `mart_category_cross_sell` | Analytical | Which categories are bought together? What's the affinity rate? |
| `mart_category_monthly_performance` | Analytical | MoM trends, online share, seasonal patterns by category |

### Partner Channel Domain
Models inspired by building a multi-region unified data model for partner analytics.

| Model | Type | What it answers |
|-------|------|-----------------|
| `dim_partners` | Dimension | Health score, revenue metrics, engagement depth per partner |
| `fct_partner_quarterly_revenue` | Fact | QoQ growth by partner × product line × region |
| `mart_region_summary` | Analytical | Cross-region executive view: revenue, adoption, at-risk partners |

## Stack

- **dbt Core** — transformations, testing, documentation
- **DuckDB** — local analytical database (zero infrastructure)
- **GitHub Actions** — CI pipeline (seed → run → test on every push)

## Quick Start

```bash
pip install dbt-core dbt-duckdb
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## Data Quality

- **Source tests**: uniqueness, referential integrity, accepted values on all raw tables
- **Schema tests**: not_null, unique, relationships across staging and mart models
- **Custom tests**: positive revenue validation, health score bounds, cross-sell self-pair detection
- **Custom generic test**: `metric_not_negative` — reusable across any numeric column

## Analyses

- `category_affinity_insights.sql` — Which beauty categories drive cross-sell revenue?
- `partner_expansion_opportunities.sql` — Which partners are engaged but under-adopted?

## Design Decisions

See [docs/decisions.md](docs/decisions.md) for rationale on star schema vs OBT, health score weighting, and data quality philosophy.

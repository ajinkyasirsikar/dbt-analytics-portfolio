# Contributing Guide

This document explains how to extend the project with new data sources, models, or domains.

## Adding a New Data Source

1. **Add seed CSV** to `seeds/` with a descriptive prefix (e.g., `billing_invoices.csv`)
2. **Create source definition** in a `_sources.yml` file under the appropriate domain's `staging/` folder
3. **Add schema tests** on the source: at minimum `unique` and `not_null` on the primary key, plus `relationships` on any foreign keys
4. **Run `dbt seed`** to load the data

## Adding a New Staging Model

1. Create `stg_{domain}_{entity}.sql` in the domain's `staging/` folder
2. Follow the pattern: `source CTE → cleaned CTE → select`
3. Cast all types explicitly — don't rely on inference
4. Add the model to the domain's `_sources.yml` with column-level tests
5. Run `dbt run -s stg_{model}` and `dbt test -s stg_{model}`

## Adding a New Mart Model

1. Determine if it's a **fact** (events/transactions), **dimension** (entities), or **analytical mart** (aggregated for a specific use case)
2. Use `ref()` to depend on staging or intermediate models — never reference sources directly from marts
3. If your mart shares aggregation logic with another mart, extract it into an **intermediate (ephemeral)** model first
4. Add to the domain's `_marts.yml` with column descriptions and tests
5. If the mart is high-volume, consider `materialized='incremental'` with a clear `unique_key` and `is_incremental()` filter

## Adding a New Domain

1. Create `models/{domain_name}/staging/` and `models/{domain_name}/marts/`
2. Add the domain to `dbt_project.yml` under `models:` with appropriate materialization defaults
3. Add seed data, source definitions, staging models, then marts
4. Add an exposure in `models/exposures.yml` to document what consumes the domain's data

## Code Standards

- SQL keywords: lowercase (`select`, `from`, `where`)
- CTEs: descriptive names (`customer_metrics`, not `t1`)
- Every model needs at least `unique` + `not_null` on its primary key
- Custom business logic tests go in `tests/`
- Run `dbt build` (seed + run + test) before committing

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

## Why ephemeral intermediate models aren't used here

For a project this size, the staging → mart path is direct enough. In production at scale, I'd add intermediate models when:
- Multiple marts share the same join logic (DRY)
- A transformation is complex enough to deserve its own test boundary
- You need to isolate a performance bottleneck

## Data quality philosophy

Tests are organized at three levels:
1. **Source tests** — validate raw data contracts (uniqueness, referential integrity, accepted values)
2. **Model tests** — validate transformation logic (positive revenue, score bounds)
3. **Business logic tests** — validate analytical correctness (no self-pairs in cross-sell, customer-order consistency)

This mirrors how I built the Schema Monitor at AWS: catch issues at the earliest possible layer, not after they've propagated to dashboards.

## Health score design (partner analytics)

The composite score (30-100) weights three signals:
- **Revenue consistency** (30 pts max): Are they paying regularly?
- **Product adoption** (30 pts max): Are they using multiple product lines?
- **Engagement depth** (40 pts max): Are they actually using the platform?

Engagement is weighted highest because API usage is the best leading indicator of churn — revenue is a lagging indicator. A partner can be paying but not using; that's a retention risk.

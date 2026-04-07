-- Generic test: detects when a metric deviates beyond a threshold from its rolling average
-- Mirrors the anomaly detection philosophy from production data quality monitoring
-- Usage in schema.yml:
--   tests:
--     - metric_variance:
--         metric_column: net_revenue
--         partition_column: category
--         order_column: txn_date
--         variance_threshold: 50  (percent)

{% test metric_variance(model, metric_column, partition_column, order_column, variance_threshold=50) %}

with with_rolling as (
    select
        {{ metric_column }},
        {{ partition_column }},
        {{ order_column }},
        avg({{ metric_column }}) over (
            partition by {{ partition_column }}
            order by {{ order_column }}
            rows between 3 preceding and 1 preceding
        ) as rolling_avg
    from {{ model }}
    where {{ metric_column }} is not null
)

select
    {{ partition_column }},
    {{ order_column }},
    {{ metric_column }},
    rolling_avg,
    round(abs({{ metric_column }} - rolling_avg) * 100.0 / nullif(rolling_avg, 0), 1) as variance_pct
from with_rolling
where rolling_avg is not null
  and abs({{ metric_column }} - rolling_avg) * 100.0 / nullif(rolling_avg, 0) > {{ variance_threshold }}

{% endtest %}

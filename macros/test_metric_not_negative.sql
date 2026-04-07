-- Generic test: ensures a metric column has no negative values
-- Reusable across both domains — apply to any revenue/profit column
-- Usage in schema.yml:
--   tests:
--     - metric_not_negative

{% test metric_not_negative(model, column_name) %}

select
    {{ column_name }} as failing_value,
    *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}

-- Reusable macro for rolling averages in time-series models
-- Demonstrates macro authoring with configurable window size

{% macro rolling_average(column_name, partition_by, order_by, window_size=3) %}
    round(
        avg({{ column_name }}) over (
            partition by {{ partition_by }}
            order by {{ order_by }}
            rows between {{ window_size - 1 }} preceding and current row
        ),
        2
    )
{% endmacro %}

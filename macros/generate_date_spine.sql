-- Generates a date spine for gap-filling in time series models
-- Useful when you need every date represented even if no transactions occurred

{% macro generate_date_spine(start_date, end_date) %}

    with date_spine as (
        select
            unnest(
                generate_series(
                    cast('{{ start_date }}' as date),
                    cast('{{ end_date }}' as date),
                    interval '1 day'
                )
            ) as date_day
    )
    select cast(date_day as date) as date_day from date_spine

{% endmacro %}

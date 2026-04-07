with source as (
    select * from {{ source('raw', 'partner_engagement') }}
),

cleaned as (
    select
        engagement_id,
        partner_id,
        cast(event_date as date) as event_date,
        lower(trim(event_type)) as event_type,
        lower(trim(product_line)) as product_line,
        cast(metric_value as decimal(12, 2)) as metric_value,
        lower(trim(metric_unit)) as metric_unit,

        extract(year from cast(event_date as date)) as event_year,
        extract(month from cast(event_date as date)) as event_month
    from source
)

select * from cleaned

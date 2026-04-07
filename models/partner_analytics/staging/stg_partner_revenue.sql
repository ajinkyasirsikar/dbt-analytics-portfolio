with source as (
    select * from {{ source('raw', 'partner_revenue') }}
),

cleaned as (
    select
        revenue_id,
        partner_id,
        cast(revenue_date as date) as revenue_date,
        lower(trim(revenue_type)) as revenue_type,
        lower(trim(product_line)) as product_line,
        cast(amount_usd as decimal(12, 2)) as amount_usd,
        upper(trim(currency_original)) as currency_original,
        cast(fx_rate as decimal(10, 6)) as fx_rate,
        cast(is_recurring as boolean) as is_recurring,

        extract(year from cast(revenue_date as date)) as revenue_year,
        extract(quarter from cast(revenue_date as date)) as revenue_quarter,
        extract(month from cast(revenue_date as date)) as revenue_month
    from source
)

select * from cleaned

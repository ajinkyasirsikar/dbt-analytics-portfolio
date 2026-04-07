with source as (
    select * from {{ source('raw', 'beauty_transactions') }}
),

cleaned as (
    select
        txn_id,
        customer_id,
        product_id,
        cast(txn_date as date) as txn_date,
        quantity,
        cast(unit_price as decimal(10, 2)) as unit_price,
        lower(trim(channel)) as channel,
        coalesce(cast(discount_pct as decimal(5, 2)), 0) as discount_pct,
        trim(store_id) as store_id,

        -- computed fields
        cast(quantity * unit_price as decimal(10, 2)) as gross_revenue,
        round(quantity * unit_price * (1 - coalesce(discount_pct, 0) / 100), 2) as net_revenue,

        extract(year from cast(txn_date as date)) as txn_year,
        extract(month from cast(txn_date as date)) as txn_month,
        extract(quarter from cast(txn_date as date)) as txn_quarter
    from source
)

select * from cleaned

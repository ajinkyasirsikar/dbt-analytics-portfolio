with source as (
    select * from {{ source('raw', 'beauty_products') }}
),

cleaned as (
    select
        product_id,
        trim(product_name) as product_name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        trim(brand) as brand,
        cast(price as decimal(10, 2)) as retail_price,
        cast(cost as decimal(10, 2)) as unit_cost,
        round((price - cost) / nullif(price, 0) * 100, 1) as gross_margin_pct,
        cast(launch_date as date) as launch_date,
        cast(is_active as boolean) as is_active,

        -- price tier for segmentation
        case
            when price >= 80 then 'prestige'
            when price >= 40 then 'mid-range'
            else 'accessible'
        end as price_tier
    from source
)

select * from cleaned

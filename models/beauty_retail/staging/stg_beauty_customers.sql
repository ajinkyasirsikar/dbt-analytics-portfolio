with source as (
    select * from {{ source('raw', 'beauty_customers') }}
),

cleaned as (
    select
        customer_id,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        lower(trim(email)) as email,
        cast(signup_date as date) as signup_date,
        lower(trim(loyalty_tier)) as loyalty_tier,
        trim(preferred_category) as preferred_category,
        trim(zip_code) as zip_code,

        -- derive region from zip for geo analysis
        case
            when zip_code like '94%' then 'SF Bay Area'
            when zip_code like '100%' then 'New York'
            when zip_code like '606%' then 'Chicago'
            else 'Other'
        end as metro_area
    from source
)

select * from cleaned

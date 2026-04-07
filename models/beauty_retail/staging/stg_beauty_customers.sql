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
        cast(zip_code as varchar) as zip_code,

        -- derive region from zip for geo analysis
        case
            when cast(zip_code as varchar) like '94%' then 'SF Bay Area'
            when cast(zip_code as varchar) like '100%' then 'New York'
            when cast(zip_code as varchar) like '606%' then 'Chicago'
            else 'Other'
        end as metro_area
    from source
)

select * from cleaned

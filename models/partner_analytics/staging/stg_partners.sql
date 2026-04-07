with source as (
    select * from {{ source('raw', 'partner_companies') }}
),

cleaned as (
    select
        partner_id,
        trim(partner_name) as partner_name,
        lower(trim(tier)) as partner_tier,
        trim(region) as region,
        trim(country) as country_code,
        trim(industry) as industry,
        cast(onboarded_date as date) as onboarded_date,
        trim(account_manager) as account_manager,
        cast(is_active as boolean) as is_active,

        -- region grouping for cross-region reporting
        case
            when region like 'NA%' then 'Americas'
            when region like 'EMEA%' then 'EMEA'
            when region = 'APAC' then 'APAC'
        end as region_group,

        -- tenure in days
        current_date - cast(onboarded_date as date) as tenure_days
    from source
)

select * from cleaned

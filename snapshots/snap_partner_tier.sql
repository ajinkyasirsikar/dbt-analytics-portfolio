-- SCD Type 2 snapshot: tracks partner tier changes over time
-- Enables analysis: "Did revenue grow after a partner was upgraded to premier?"

{% snapshot snap_partner_tier %}

{{
    config(
        target_schema='snapshots',
        unique_key='partner_id',
        strategy='check',
        check_cols=['tier', 'is_active'],
    )
}}

select
    partner_id,
    partner_name,
    tier,
    region,
    is_active,
    account_manager
from {{ source('raw', 'partner_companies') }}

{% endsnapshot %}

-- SCD Type 2 snapshot: tracks changes to customer loyalty tier over time
-- This enables historical analysis: "What tier was this customer in when they made purchase X?"
-- Critical for accurate cohort analysis and loyalty program ROI measurement

{% snapshot snap_customer_loyalty %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['loyalty_tier', 'preferred_category'],
        invalidate_hard_deletes=True,
    )
}}

select
    customer_id,
    first_name || ' ' || last_name as full_name,
    loyalty_tier,
    preferred_category,
    email
from {{ source('raw', 'beauty_customers') }}

{% endsnapshot %}

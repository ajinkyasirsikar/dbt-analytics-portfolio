-- Health score should always be between 30 and 100 (sum of three components, each 10-40)
-- Catches logic errors in the composite scoring formula

select
    partner_id,
    partner_name,
    health_score
from {{ ref('dim_partners') }}
where health_score < 30 or health_score > 100

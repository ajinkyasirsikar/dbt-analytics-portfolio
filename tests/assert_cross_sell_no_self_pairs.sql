-- Cross-sell matrix should never pair a category with itself
-- Validates the self-join logic in mart_category_cross_sell

select *
from {{ ref('mart_category_cross_sell') }}
where category_a = category_b

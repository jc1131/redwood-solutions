WITH commission_tiers AS (
select * from {{ ref('stg_commission_form__config_commission') }}
),
int_invoice as (
    select * from {{ ref('int_invoice') }}
)
running_totals AS (
  SELECT 
    request_id,
    running_total, 
    commission_amount,
    running_total - commission_amount AS previous_total
  FROM commission_requests
)
SELECT 
  rt.request_id,
  t.min_amount,
  t.max_amount,
  t.rate,
  LEAST(rt.running_total, t.max_amount) 
    - GREATEST(rt.previous_total, t.min_amount) AS tier_amount,
  (LEAST(rt.running_total, t.max_amount) 
    - GREATEST(rt.previous_total, t.min_amount)) * t.rate AS tier_payout
FROM running_totals rt
JOIN commission_tiers t
  ON rt.running_total > t.min_amount AND rt.previous_total < t.max_amount
  and t.email_address = rt.email_address
WHERE tier_amount > 0;

with payout_combined as (
    select * from {{ ref('int_payout_combined') }}
)
select
  *
from payout_combined

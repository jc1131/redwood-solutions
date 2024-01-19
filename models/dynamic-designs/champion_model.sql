{{
  config(
    materialized='view'
  )
}}

with rfm_summary as (
  select * from exalted-slate-410901.dynamic_designs.rfm_summary
)
, rfm_category as (
  select * from exalted-slate-410901.dynamic_designs.rfm_category

),final as (
select
  case
    when rfm_s.monetary < 500 then 'Do Not Call'
    else rfm_c.rfm_category 
    end as customer_segment
  ,rfm_s.customerID as customer_name
  ,rfm_s.recency as days_since_last_order
  ,rfm_s.frequency as number_of_orders
  ,cast(rfm_s.monetary as numeric) as total_sales
  ,rfm_s.r_quartile + 1 as r_quart
  ,rfm_s.f_quartile + 1 as f_quart
from rfm_summary rfm_s
  left join rfm_category rfm_c 
    on rfm_c.rfm_score = rfm_s.RFM_Score
)
select * from final
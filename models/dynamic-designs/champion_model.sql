
with rfm_summary as (
  select * from {{ ref('stg_rfm') }}
)
, rfm_category as (
  select * from {{ ref('stg_category') }}

), stg_customer as (
  select * from {{ ref('stg_customer') }}

),final as (
select
  case
    when rfm_s.monetary < 1000 then 'Do Not Call'
    else rfm_c.rfm_category 
    end as customer_segment
    ,case 
    when rfm_s.monetary < 1000 then 'Do Not Call'
    when cust.num_year > 3 and rfm_s.monetary < 1000 then 'Loyal'
    when rfm_s.monetary > 1000 and rfm_s.monetary < 10000 then 'Loyal'
    when rfm_s.monetary > 10000 then 'Champion'
        else rfm_c.rfm_category 
    end as customer_segment_adjusted
  ,rfm_s.customerID as customer_name
  ,rfm_s.recency as days_since_last_order
  ,rfm_s.frequency as number_of_orders
  ,cast(rfm_s.monetary as numeric) as total_sales
  ,rfm_s.r_quartile + 1 as r_quart
  ,rfm_s.f_quartile + 1 as f_quart
from rfm_summary rfm_s
  left join rfm_category rfm_c 
    on rfm_c.rfm_score = rfm_s.RFM_Score
left join stg_customer cust
    on cust.customer_id = rfm_s.customerID
)
select * from final
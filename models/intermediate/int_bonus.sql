WITH base_response AS (
    SELECT * FROM {{ ref('int_response_combined') }}
),
config_commission_relationship as (
    select * from {{ ref('stg_commission_form__config_commission_relationship') }}
),
response_header as (
    select * from {{ ref('int_response_header') }}
)
select 
base_response.*
,config_commission_relationship.commission_rate
,work_start_date 
,DATE_ADD(day,config_commission_relationship.commission_hold_date,work_start_date)
,config_commission_relationship.commission_rate * invoice_amount as bonus_amount
,CONCAT('Commission from ', config_commission_relationship.primary_recruiter_name) as bonus_description
from base_response
    left join config_commission_relationship on config_commission_relationship.primary_recruiter_email = base_response.recruiter_email
where config_commission_relationship.secondary_recruiter_email is not null
/*
   SELECT
        cr.secondary_salesperson AS salesperson,
        bs.deal_amount * cr.commission_rate AS deal_amount,
        CONCAT('Commission from ', bs.primary_salesperson) AS deal_description
    FROM base_sales bs
        ON bs.primary_salesperson = cr.primary_salesperson
*/
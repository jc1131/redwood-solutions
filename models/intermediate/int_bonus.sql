WITH base_response AS (
    SELECT * FROM {{ ref('int_response_combined') }}
),
config_commission_relationship as (
    select * from {{ ref('stg_commission_form__config_commission_relationship') }}
)
select 
base_response.form_response_combine_pk
,config_commission_relationship.secondary_recruiter_name AS recruiter_name
,config_commission_relationship.commission_rate
,work_start_date 
,config_commission_relationship.commission_rate * invoice_amount as bonus_amount
,CONCAT(
    FORMAT('%g', config_commission_relationship.commission_rate * 100), '% commission from ', 
    config_commission_relationship.primary_recruiter_name, 
    ' on Job Order #', job_order_number
) AS bonus_description

from base_response
    left join config_commission_relationship on config_commission_relationship.primary_recruiter_email = base_response.recruiter_email

where config_commission_relationship.secondary_recruiter_email is not null

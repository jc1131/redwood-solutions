WITH base_response AS (
    SELECT * FROM {{ ref('int_response_combined') }}
),
config_commission_relationship as (
    select * from {{ ref('stg_commission_form__config_commission_relationship') }}
),dim_date as (
    select * from {{ ref('dim_date') }}
),
bonus_configuration as (
    select * from {{ ref('stg_commission_form__config_bonus') }}
),
secondary_commission as (
select 
{{ dbt_utils.generate_surrogate_key(['config_commission_relationship.config_commission_relationship_pk', 'job_order_number']) }} as config_bonus_pk
,config_commission_relationship.secondary_recruiter_name AS recruiter_name
,DATE_ADD(work_start_date, INTERVAL commission_hold_days DAY) AS bonus_pay_date
,config_commission_relationship.commission_rate * invoice_amount as bonus_amount
,CONCAT(
        FORMAT('%g', config_commission_relationship.commission_rate * 100), '% commission from ', 
        config_commission_relationship.primary_recruiter_name, 
        ' on Job Order #', job_order_number, 
        ' (Start Date: ', work_start_date, 
        ', Hold Days: ', commission_hold_days
)AS bonus_description

from base_response
    left join config_commission_relationship on config_commission_relationship.primary_recruiter_email = base_response.recruiter_email

where config_commission_relationship.secondary_recruiter_email is not null

), sale_ytd as (
select 
form_response_combine_pk
,due_date
,recruiter_email
,invoice_amount
,sum(invoice_amount) over (partition by recruiter_email order by due_date asc) as total_sales_ytd
from base_response


),combine_bonus_sale as (
 select 
--{{ dbt_utils.generate_surrogate_key(['config_commission_relationship.config_commission_relationship_pk', 'job_order_number']) }} as config_bonus_pk

config_bonus_pk
,employee_name
,employee_email
,bonus_end_date
,bonus_plan
,bonus_threshold
,bonus_amount
,sale_ytd.*
-- , ROW_NUMBER() OVER (
--     PARTITION BY recruiter_email,bonus_end_date,bonus_plan
--     ORDER BY due_date desc) as row_num
from sale_ytd
    join bonus_configuration on sale_ytd.recruiter_email = bonus_configuration.employee_email
    and sale_ytd.due_date between bonus_configuration.bonus_start_date and bonus_configuration.bonus_end_date
)
select * from combine_bonus_sale
qualify ROW_NUMBER() OVER (
    PARTITION BY recruiter_email,bonus_end_date,bonus_plan
    ORDER BY due_date desc) = 1
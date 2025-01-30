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
secondary_bonus as (
select 
{{ dbt_utils.generate_surrogate_key(['config_commission_relationship.config_commission_relationship_pk', 'job_order_number']) }} as secondary_bonus_pk
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
 select config_bonus_pk,
        bonus_configuration.employee_email as recruiter_email,
                bonus_configuration.employee_name as recruiter_name,

        bonus_configuration.bonus_plan,
        bonus_configuration.bonus_threshold,
        bonus_configuration.bonus_amount,
        bonus_configuration.bonus_start_date,
        bonus_configuration.bonus_end_date,
        base_response.due_date,
        SUM(base_response.invoice_amount) OVER (
            PARTITION BY bonus_configuration.employee_email, bonus_configuration.bonus_start_date, bonus_configuration.bonus_end_date
            ORDER BY base_response.due_date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS bonus_ytd,
        
from bonus_configuration
    join base_response on base_response.recruiter_email = bonus_configuration.employee_email
    and base_response.due_date between bonus_configuration.bonus_start_date and bonus_configuration.bonus_end_date
),primary_bonus as (
    select
    config_bonus_pk as primary_bonus_pk
    ,recruiter_name
    ,case 
        when bonus_ytd >= bonus_threshold then due_date
        else bonus_end_date
        end as bonus_pay_date
    ,case 
        when bonus_ytd >= bonus_threshold then bonus_amount
        else 0
        end as bonus_amount
    ,CONCAT(bonus_plan, ' payout') as bonus_description
    from combine_bonus_sale
qualify ROW_NUMBER() OVER (
     PARTITION BY config_bonus_pk
     ORDER BY bonus_pay_date asc) = 1 
),

retro_bonus as (
    select
    *
    from bonus_configuration
        left join primary_bonus on    primary_bonus.primary_bonus_pk = bonus_configuration.config_bonus_pk
),
    
    final as (
    select * from secondary_bonus
    union all 
    select * from primary_bonus
    -- union all 
    -- select * from retro_bonus
)
select * from retro_bonus 


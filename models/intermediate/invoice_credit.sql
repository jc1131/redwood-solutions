with source_form as (
    select * from {{ ref('stg_commission_form__form_response') }} where invoice_amount is not null
),
source_recruiter as (
    select * from {{ ref('int_recruiter') }}
),
combine_credit as (
SELECT 
job_order_number
,invoice_amount
  ,agreement_job_order_percentage 
  ,agreement_job_order_recruiter 
  ,invoice_amount * agreement_job_order_percentage as invoice_credit
  ,'Agreement/Job Order' as job_order_role

 FROM source_form
 
union all
SELECT job_order_number
,invoice_amount
  ,account_manager_percentage
  ,account_manager_recruiter
  ,invoice_amount * account_manager_percentage as invoice_credit
    ,'Account Manager' as job_order_role

 FROM source_form
 union all
SELECT job_order_number
,invoice_amount
  ,working_candidate_percentage
  ,working_candidate_recruiter
  ,invoice_amount * working_candidate_percentage as invoice_credit
  ,'Working Candidate' as job_order_role

 FROM source_form
 union all
SELECT job_order_number
,invoice_amount
  ,candidate_ownership_percentage
  ,candidate_ownership_recruiter
  ,invoice_amount * candidate_ownership_percentage as invoice_credit
  ,'Candidate Ownership' as job_order_role

 FROM source_form
),rename_credit as (
    select
    job_order_number
    ,invoice_amount
    ,agreement_job_order_percentage as recruiter_credit_percentage
    ,agreement_job_order_recruiter as recruiter_name
    ,invoice_credit as credit_amount
    ,job_order_role
    ,CONCAT(FORMAT('%.0f%%', agreement_job_order_percentage * 100), ' credit for ', job_order_role) AS credit_description
    ,SUM(agreement_job_order_percentage) OVER (PARTITION BY job_order_number) = 1.0 AS is_valid_percent_allocation
    from combine_credit
    group by all
),pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['job_order_number', 'source_recruiter.recruiter_email','job_order_role']) }} as invoice_credit_pk
    ,source_recruiter.recruiter_email
    ,rename_credit.*
    from rename_credit
        left join source_recruiter on source_recruiter.recruiter_name = rename_credit.recruiter_name
)
select * from pk_generation

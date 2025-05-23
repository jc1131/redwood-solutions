with source_form as (
    select * from {{ ref('stg_commission_form__form_response') }} where invoice_amount is not null
),lookup_recruiter as (
    select * from {{ ref('stg_commission_form__config_commission_relationship') }}
),
combine_credit as (
SELECT 
form_response_pk
,invoice_amount
  ,agreement_job_order_percentage 
  ,agreement_job_order_recruiter 
  ,'Agreement/Job Order' as job_order_role

 FROM source_form
 
union all
SELECT form_response_pk
,invoice_amount
  ,account_manager_percentage
  ,account_manager_recruiter
    ,'Account Manager' as job_order_role

 FROM source_form
 union all
SELECT form_response_pk
,invoice_amount
  ,working_candidate_percentage
  ,working_candidate_recruiter
  ,'Working Candidate' as job_order_role

 FROM source_form
 union all
SELECT form_response_pk
,invoice_amount
  ,candidate_ownership_percentage
  ,candidate_ownership_recruiter
  ,'Candidate Ownership' as job_order_role

 FROM source_form
),rename_credit as (
    select
    form_response_pk
    ,invoice_amount
    ,agreement_job_order_percentage as recruiter_credit_percentage
    ,agreement_job_order_recruiter as recruiter_name
    ,job_order_role
    ,CONCAT(FORMAT('%.0f%%', agreement_job_order_percentage * 100), ' credit for ', job_order_role) AS form_detail_description
    ,SUM(agreement_job_order_percentage) OVER (PARTITION BY form_response_pk) = 1.0 AS is_valid_percentage
    from combine_credit
    group by all
),pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['form_response_pk', 'lookup_recruiter.primary_recruiter_email','job_order_role']) }} as form_response_detail_pk
    ,form_response_pk as form_response_fk
    ,lookup_recruiter.primary_recruiter_email as recruiter_email
    ,rename_credit.recruiter_credit_percentage
    ,rename_credit.recruiter_name
    ,rename_credit.job_order_role
    ,rename_credit.form_detail_description
    ,is_valid_percentage
    from rename_credit
        left join lookup_recruiter on lookup_recruiter.primary_recruiter_name = rename_credit.recruiter_name
)
select * from pk_generation

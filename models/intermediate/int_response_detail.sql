with source_form as (
    select * from {{ ref('stg_commission_form__form_response') }} where invoice_amount is not null
),lookup_recruiter as (
    select * from {{ ref('int_recruiter') }}
),
combine_credit as (
SELECT 
form_response_pk
,invoice_amount
  ,agreement_job_order_percentage 
  ,agreement_job_order_recruiter 
  ,invoice_amount * agreement_job_order_percentage as invoice_credit
  ,'Agreement/Job Order' as job_order_role

 FROM source_form
 
union all
SELECT form_response_pk
,invoice_amount
  ,account_manager_percentage
  ,account_manager_recruiter
  ,invoice_amount * account_manager_percentage as invoice_credit
    ,'Account Manager' as job_order_role

 FROM source_form
 union all
SELECT form_response_pk
,invoice_amount
  ,working_candidate_percentage
  ,working_candidate_recruiter
  ,invoice_amount * working_candidate_percentage as invoice_credit
  ,'Working Candidate' as job_order_role

 FROM source_form
 union all
SELECT form_response_pk
,invoice_amount
  ,candidate_ownership_percentage
  ,candidate_ownership_recruiter
  ,invoice_amount * candidate_ownership_percentage as invoice_credit
  ,'Candidate Ownership' as job_order_role

 FROM source_form
),rename_credit as (
    select
    form_response_pk
    ,invoice_amount
    ,agreement_job_order_percentage as recruiter_credit_percentage
    ,agreement_job_order_recruiter as recruiter_name
    ,invoice_credit as credit_amount
    ,job_order_role
    ,CONCAT(FORMAT('%.0f%%', agreement_job_order_percentage * 100), ' credit for ', job_order_role) AS form_detail_description
    from combine_credit
),pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['form_response_pk', 'lookup_recruiter.recruiter_email','job_order_role']) }} as form_response_detail_pk
    ,form_response_pk as form_response_fk
    ,lookup_recruiter.recruiter_email
    ,rename_credit.recruiter_credit_percentage
    ,rename_credit.recruiter_name
    ,rename_credit.credit_amount
    ,rename_credit.job_order_role
    ,rename_credit.form_detail_description
    from rename_credit
        left join lookup_recruiter on lookup_recruiter.recruiter_name = rename_credit.recruiter_name
)
select * from pk_generation

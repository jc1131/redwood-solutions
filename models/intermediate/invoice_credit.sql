SELECT invoice_amount
  ,agreement_job_order_percentage
  ,agreement_job_order_recruiter
    ,invoice_amount * agreement_job_order_percentage as invoice_credit

 FROM `exalted-slate-410901.dbt_jcrist.stg_commission_form__form_response` 
where form_response_pk = '47b59bea7112e9dbc8a3ef97963e7774'

union all
SELECT invoice_amount
  ,working_candidate_percentage
  ,working_candidate_recruiter
  ,invoice_amount * working_candidate_percentage as invoice_credit
 FROM `exalted-slate-410901.dbt_jcrist.stg_commission_form__form_response` 

where form_response_pk = '47b59bea7112e9dbc8a3ef97963e7774'

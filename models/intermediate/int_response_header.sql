with source_invoice as (
    select * from {{ ref('stg_commission_form__form_response') }}
)
select
form_response_pk
,last_modified
,job_order_number
,client_name
,is_new_w9
,client_billing_address
,is_fee_calculation
,remit_client_billing
,candidate_name
,position_title
,client_hiring_manager
,billing_percentage_rate
,candidate_base_salary
,work_start_date
,offer_signature_date
,invoice_payment_terms
,invoice_payment_date
, 
  CASE 
    WHEN invoice_payment_date = 'Work Start Date' THEN DATE_ADD(work_start_date, INTERVAL payment_term_number DAY)
    WHEN invoice_payment_date = 'Offer Signature Date' THEN DATE_ADD(offer_signature_date, INTERVAL payment_term_number DAY)
    ELSE NULL -- Handle unexpected cases
  END AS due_date

 from source_invoice
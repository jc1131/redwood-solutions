with response_header as (
    select * from {{ ref('int_response_header') }}
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
,due_date
,invoice_amount
 from response_header
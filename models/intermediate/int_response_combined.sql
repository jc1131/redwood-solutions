WITH response_header as (
    select * from {{ ref('int_response_header') }}
),
response_detail as (
    select * from {{ ref('int_response_detail') }}
)
SELECT 
{{ dbt_utils.generate_surrogate_key(['response_header.form_response_pk', 'response_detail.form_response_detail_pk']) }} as form_response_combine_pk
,response_header.form_response_pk as form_response_fk
,response_detail.form_response_detail_pk as form_response_detail_fk
,response_header.last_modified
,response_header.job_order_number
,response_header.client_name
,response_header.is_new_w9
,response_header.client_billing_address
,response_header.is_fee_calculation
,response_header.remit_client_billing
,response_header.candidate_name
,response_header.position_title
,response_header.client_hiring_manager
,response_header.billing_percentage_rate
,response_header.candidate_base_salary
,response_header.work_start_date
,response_header.offer_signature_date
,response_header.invoice_payment_terms
,response_header.invoice_payment_date
,response_header.due_date
,response_header.invoice_amount
,SUM(response_detail.credit_amount) OVER (PARTITION BY response_detail.recruiter_email ORDER BY response_header.due_date) AS running_total
,response_detail.recruiter_email
,response_detail.recruiter_credit_percentage
,response_detail.recruiter_name
,response_detail.credit_amount
,response_detail.job_order_role
,response_detail.form_detail_description
,response_detail.is_valid_percentage

FROM response_header
    left join response_detail on response_detail.form_response_fk = response_header.form_response_pk
group by all

WITH response_header as (
    select * from {{ ref('int_response_header') }}
),
response_detail as (
    select * from {{ ref('int_response_detail') }}
)
SELECT 
response_header.form_response_pk,
response_header.due_date,
response_detail.recruiter_email, 
response_header.invoice_amount,
response_detail.credit_amount,
SUM(response_detail.credit_amount) OVER (PARTITION BY response_detail.recruiter_email ORDER BY response_header.due_date) AS running_total
FROM response_header
    left join response_detail on response_detail.form_response_fk = response_header.form_response_pk
group by all
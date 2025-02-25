 /*
    SELECT
    form_response_combine_fk,
    NULL AS last_modified_date,
    recruiter_name as recruiter_name,
    NULL AS job_number,
    NULL AS company,
    NULL AS candidate_name,
    due_date AS due_date,
    invoice_amount AS invoice_amount,
    SUM(invoice_amount) OVER (PARTITION BY recruiter_name ORDER BY due_date) AS total_invoice_ytd,
    commission_percentage AS commission_percentage,
    sum(tier_commission) AS commission_amount,
    NULL AS other_comm_and_bonus,
    due_date AS due_date,
    NULL AS date_paid,
    NULL AS notes,

    */
with form_combine as (
    select * from {{ ref('int_response_combined') }}
),
form_payout as (
    select * from {{ ref('int_payout_combined') }}
)

select
 form_combine.form_response_combine_pk,
    form_combine.last_modified AS last_modified_date,
    form_combine.recruiter_name as recruiter_name,
    form_combine.job_order_number AS job_number,
    form_combine.client_name AS company,
    form_combine.candidate_name AS candidate_name,
    form_payout.invoice_amount,
    form_payout.total_invoice_ytd,
    form_payout.commission_percentage AS commission_percentage,
    form_payout.commission_amount AS commission_amount,
    form_payout.other_comm_and_bonus AS other_comm_and_bonus,
    form_payout.due_date AS due_date,
    NULL AS date_paid,
    NULL AS notes,
    payout_description,
    form_detail_description,
from form_combine
    left join form_payout on form_combine.form_response_combine_pk = form_payout.form_response_combine_fk
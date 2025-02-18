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
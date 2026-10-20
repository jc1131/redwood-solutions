with manual_adjustment as (

    select
        {{ dbt_utils.generate_surrogate_key(["'manual-1'", "'Gayle Simons'"]) }} as form_response_pk,
        'Gayle Simons' as recruiter_name,
        DATE '2026-09-30' as inv_date,
        '3032' as job_order_number,
        'Center for the Collaborative Classroom' as client_name,
        'Mallery Erickson' as candidate_name,
        DATE '2026-10-20' as inv_due_date,
        15000 as invoice_amount,
        1.0 as credit_percentage,
        15000 as credit_amount,
        'manual adjustment' as split_description

    union all

    select
        {{ dbt_utils.generate_surrogate_key(["'manual-1'", "'Lucas Ingmire'"]) }} as form_response_pk,
        'Lucas Ingmire' as recruiter_name,
        DATE '2026-09-30' as inv_date,
        '3032' as job_order_number,
        'Center for the Collaborative Classroom' as client_name,
        'Mallery Erickson' as candidate_name,
        DATE '2026-10-20' as inv_due_date,
        -5000 as invoice_amount,
        1.0 as credit_percentage,
        -5000 as credit_amount,
        'manual adjustment' as split_description

),

    
    int_invoice_detail as (

    select * from {{ ref('int_invoice_detail') }}

),
    invoice_detail as (

    select
        form_response_pk,
        recruiter_name,
        inv_date,
        job_order_number,
        client_name,
        candidate_name,
        inv_due_date,
        invoice_amount,
        credit_percentage,
        credit_amount,
        split_description
    from int_invoice_detail

    union all

    select
        form_response_pk,
        recruiter_name,
        inv_date,
        job_order_number,
        client_name,
        candidate_name,
        inv_due_date,
        invoice_amount,
        credit_percentage,
        credit_amount,
        split_description
    from manual_adjustment
),


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Aggregate all recruiter split roles to one invoice row
-- Grain = (invoice × recruiter)
-- ─────────────────────────────────────────────────────────────────────────────
aggregated as (

    select
        invoice_detail.form_response_pk,
        invoice_detail.recruiter_name,
        invoice_detail.inv_date as invoice_date,
        invoice_detail.job_order_number as bullhorn_job_order_number,
        invoice_detail.client_name as company,
        invoice_detail.candidate_name,
        invoice_detail.inv_due_date as invoice_due_date,
        round(invoice_detail.invoice_amount,2) as invoice_total,


        -- Aggregate all recruiter splits on same invoice
        sum(invoice_detail.credit_percentage)    as recruiter_percentage_of_sale,
        ROUND(sum(invoice_detail.credit_amount),2)        as commissionable_sales,

        string_agg(
            invoice_detail.split_description,
            ' | '
        )                                        as notes

    from invoice_detail


    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Recruiter YTD credited sales
-- ─────────────────────────────────────────────────────────────────────────────
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'form_response_pk',
            'recruiter_name'
        ]) }}                                     as commission_pk,

        aggregated.*,
        -- Running YTD based on recruiter's sales amount
        sum(invoice_total)
            over (
                partition by recruiter_name
                order by invoice_date
            )                                     as invoice_total_ytd,
        -- Running YTD based on recruiter's credited split amount
        sum(commissionable_sales)
            over (
                partition by recruiter_name
                order by invoice_date, bullhorn_job_order_number
                    rows between unbounded preceding and current row
            )                                     as total_sales_ytd,

    from aggregated

)

select * from final

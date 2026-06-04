with invoice_detail as (

    select * from {{ ref('int_invoice_detail') }}

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
                order by invoice_date
            )                                     as total_sales_ytd,

    from aggregated

)

select * from final

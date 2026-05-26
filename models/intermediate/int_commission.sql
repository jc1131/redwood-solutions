with invoice_detail as (

    select * from {{ ref('int_invoice_detail') }}

),

int_date as (

    select * from {{ ref('dim_date') }}

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Aggregate all recruiter split roles to one invoice row
-- Grain = (invoice × recruiter)
-- ─────────────────────────────────────────────────────────────────────────────
aggregated as (

    select
        invoice_detail.form_response_pk,
        invoice_detail.job_order_number,
        invoice_detail.client_name,
        invoice_detail.candidate_name,

        round(invoice_detail.invoice_amount,2) as invoice_amount,

        int_date.year_number                     as offer_signature_date_year,
        invoice_detail.offer_signature_date,

        invoice_detail.recruiter_name,
        invoice_detail.recruiter_email,

        -- Aggregate all recruiter splits on same invoice
        sum(invoice_detail.credit_percentage)    as invoice_split_credit_percent,
        ROUND(sum(invoice_detail.credit_amount),2)        as invoice_split_credit_amount,

        string_agg(
            invoice_detail.split_description,
            ' | '
        )                                        as form_detail_description

    from invoice_detail
    left join int_date
        on int_date.date_day = invoice_detail.offer_signature_date

    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Recruiter YTD credited sales
-- ─────────────────────────────────────────────────────────────────────────────
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'form_response_pk',
            'recruiter_email'
        ]) }}                                     as commission_pk,

        aggregated.*,

        -- Running YTD based on recruiter's credited split amount
        sum(invoice_split_credit_amount)
            over (
                partition by recruiter_email
                order by offer_signature_date
            )                                     as total_commission_sales

    from aggregated

)

select * from final

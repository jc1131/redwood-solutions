/*
  int_invoice_detail
  ──────────────────
  Grain  : one row per (invoice × recruiter credit role).
  Sources: stg_commission_form__form_response (single scan)
           stg_commission_form__config_commission_relationship (email lookup)
*/

with form_response as (

    select * from {{ ref('stg_commission_form__form_response') }}
    where invoice_amount is not null

),

/*
  Derive due_date in its own CTE so the alias is unambiguously available
  to the unpivot UNION branches below. Referencing it directly inside
  a downstream CTE alias would fail in BigQuery when the model is
  compiled as ephemeral (inlined SQL).
*/
invoice_with_due_date as (

    select
        *,
        case
            when invoice_payment_date = 'Work Start Date'
                then date_add(work_start_date,      interval payment_term_number day)
            when invoice_payment_date = 'Offer Signature Date'
                then date_add(offer_signature_date, interval payment_term_number day)
            else null
        end as due_date
    from form_response

),

commission_relationship as (

    select
        primary_recruiter_name,
        primary_recruiter_email
    from {{ ref('stg_commission_form__config_commission_relationship') }}

),

unpivoted as (

    select
        form_response_pk,
        job_order_number,
        client_name,
        candidate_name,
        invoice_amount,
        due_date,
        last_modified,
        agreement_job_order_percentage  as credit_percentage,
        agreement_job_order_recruiter   as recruiter_name,
        'Agreement/Job Order'           as credit_role
    from invoice_with_due_date

    union all

    select
        form_response_pk,
        job_order_number,
        client_name,
        candidate_name,
        invoice_amount,
        due_date,
        last_modified,
        account_manager_percentage,
        account_manager_recruiter,
        'Account Manager'
    from invoice_with_due_date

    union all

    select
        form_response_pk,
        job_order_number,
        client_name,
        candidate_name,
        invoice_amount,
        due_date,
        last_modified,
        working_candidate_percentage,
        working_candidate_recruiter,
        'Working Candidate'
    from invoice_with_due_date

    union all

    select
        form_response_pk,
        job_order_number,
        client_name,
        candidate_name,
        invoice_amount,
        due_date,
        last_modified,
        candidate_ownership_percentage,
        candidate_ownership_recruiter,
        'Candidate Ownership'
    from invoice_with_due_date

),

active_splits as (

    select *
    from unpivoted
    where
        recruiter_name is not null
        and trim(recruiter_name) != ''
        and credit_percentage > 0

),

enriched as (

    select
        active_splits.form_response_pk,
        active_splits.job_order_number,
        active_splits.client_name,
        active_splits.candidate_name,
        active_splits.invoice_amount,
        active_splits.due_date,
        active_splits.last_modified,
        active_splits.recruiter_name,
        active_splits.credit_role,
        active_splits.credit_percentage,
        active_splits.credit_percentage
            * active_splits.invoice_amount                      as credit_amount,
        commission_relationship.primary_recruiter_email         as recruiter_email,
        sum(active_splits.credit_percentage)
            over (partition by active_splits.form_response_pk) = 1.0
                                                                as is_valid_split,
        concat(
            format('%.0f%%', active_splits.credit_percentage * 100),
            ' credit — ', active_splits.credit_role
        )                                                       as split_description
    from active_splits
    left join commission_relationship
        on commission_relationship.primary_recruiter_name
            = active_splits.recruiter_name

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'form_response_pk',
            'recruiter_email',
            'credit_role'
        ]) }}                   as invoice_detail_pk,
        *
    from enriched

)

select * from final
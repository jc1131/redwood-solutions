/*
  stg_commission_form__form_response
  ──────────────────────────────────
  Grain  : one row per job order (latest submission wins).
  Purpose: rename raw columns, cast types, derive invoice_amount and
           payment_term_number, and deduplicate on job_order_number.

  Key derivations
  ───────────────
  invoice_amount      = billing_percentage_rate × candidate_base_salary
  payment_term_number = integer extracted from invoice_payment_terms string
                        e.g. "Net 30" → 30

  Note: due_date is derived in int_invoice_detail (not here) because
  BigQuery cannot resolve a CTE alias from a prior CTE when ephemeral
  models are inlined during compilation.
*/

with source as (

    select * from {{ source('commission_form', 'form_response') }}

),

renamed as (

    select
        timestamp                                                       as last_modified,
        email_address                                                   as submitted_by_email,
        job_order_number,
        client_name,
        new_client_needing_fieldpros_w9_                                as is_new_w9,
        client_billing_address,
        should_the_invoice_description_include_the_fee_calculation__including_the_candidate_s_salary_
                                                                        as is_fee_calculation,
        remit_client_billing_                                           as remit_client_billing,
        candidate_name,
        position_title,
        client_hiring_manager,
        billing_percentage_rate,
        candidate_s_base_salary                                         as candidate_base_salary,
        billing_percentage_rate * candidate_s_base_salary               as invoice_amount,
        work_start_date,
        offer_signature_date,
        invoice_payment_terms,
        cast(
            regexp_extract(invoice_payment_terms, r'\b\d+\b') as int64
        )                                                               as payment_term_number,
        reference_date_for_terms                                        as invoice_payment_date,
        agreement_job_order_percentage,
        agreement_job_order_recruiter,
        account_manager_percentage,
        account_manager_recruiter,
        working_candidate_percentage_                                   as working_candidate_percentage,
        working_candidate_recruiter,
        candidate_ownership_percentage_                                 as candidate_ownership_percentage,
        candidate_ownership_recruiter,
        'form_response'                                                 as source_key,
        row_number() over ()                                            as source_row_number
    from source

),

deduplicated as (

    select *
    from renamed
    qualify
        row_number() over (
            partition by job_order_number
            order by last_modified desc
        ) = 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }}
            as form_response_pk,
        *
    from deduplicated

)

select * from final
where last_modified > '2026-01-01'
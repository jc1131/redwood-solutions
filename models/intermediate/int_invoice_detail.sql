/*
  int_invoice_detail
  ──────────────────
  Grain  : one row per (invoice × recruiter credit role).
  Sources: stg_commission_form__form_response (single scan)
           stg_commission_form__config_commission_relationship (email lookup)

  What this model does
  ────────────────────
  1. UNPIVOT — the four credit-role column pairs on each form_response row
     (Agreement/Job Order, Account Manager, Working Candidate, Candidate
     Ownership) are unpivoted into individual rows. This is the only place
     this transformation happens.

  2. FILTER — rows where the recruiter name is blank or the credit percentage
     is zero are removed (the role was unused on that invoice).

  3. ENRICH — join config_commission_relationship to resolve each recruiter's
     canonical email address, which is the join key used in int_commission
     for tier lookups.

  4. VALIDATE — a boolean flag (is_valid_split) signals whether the credit
     percentages on an invoice sum to exactly 100 %. Downstream models surface
     this flag but do not filter on it — data-quality issues are visible in
     the mart.

  What this model does NOT do
  ───────────────────────────
  - No commission calculation
  - No running totals
  - No pay-date logic
*/

with form_response as (

    select * from {{ ref('stg_commission_form__form_response') }}
    where invoice_amount is not null

),

commission_relationship as (

    select
        primary_recruiter_name,
        primary_recruiter_email
    from {{ ref('stg_commission_form__config_commission_relationship') }}

),

/*
  Unpivot the four role pairs into rows.
  Each UNION branch represents one credit role.
*/
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
    from form_response

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
    from form_response

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
    from form_response

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
    from form_response

),

/*
  Remove unused role slots — blank recruiter name or zero credit %.
*/
active_splits as (

    select *
    from unpivoted
    where
        recruiter_name is not null
        and trim(recruiter_name) != ''
        and credit_percentage > 0

),

/*
  Enrich with recruiter email and add a per-invoice validation flag.
  The SUM window across form_response_pk tells us if splits are complete.
*/
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

        -- Dollar amount this recruiter is credited for this invoice
        active_splits.credit_percentage
            * active_splits.invoice_amount                      as credit_amount,

        -- Canonical email used for tier lookups in int_commission
        commission_relationship.primary_recruiter_email         as recruiter_email,

        -- Data-quality flag: do all splits on this invoice add to 100%?
        sum(active_splits.credit_percentage)
            over (partition by active_splits.form_response_pk) = 1.0
                                                                as is_valid_split,

        -- Human-readable credit description for payout_description column
        concat(
            format('%.0f%%', active_splits.credit_percentage * 100),
            ' credit — ', active_splits.credit_role
        )                                                       as split_description

    from active_splits
    left join commission_relationship
        on commission_relationship.primary_recruiter_name
            = active_splits.recruiter_name

),

/*
  Generate a surrogate PK scoped to (invoice, recruiter email, role).
*/
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
/*
  int_commission
  ──────────────
  Grain  : one row per (invoice × recruiter).
  Sources: int_invoice_detail
           stg_commission_form__config_commission
           stg_commission_form__config_commission_relationship
           stg_commission_form__form_payment

  What this model does
  ────────────────────
  STEP 1 — AGGREGATE splits to one row per (invoice × recruiter).
    A recruiter may hold multiple roles on the same invoice
    (e.g. Agreement/Job Order + Account Manager). We sum their
    credit_percentage and credit_amount across roles before applying
    the tier calc, so the recruiter sees one commission row per invoice.
    Filtered to current calendar year only so YTD totals reset each Jan 1.

  STEP 2 — RUNNING TOTALS per recruiter ordered by due_date.
    total_commission_sales  = recruiter's cumulative credited $ (current year)
    total_invoice_ytd       = recruiter's cumulative gross invoice $ (current year)
    These drive which commission tier applies to each invoice.

  STEP 3 — TIER CALCULATION via range join to config_commission.
    For each invoice, one or more tier bands may be "touched" depending
    on whether the invoice straddles a tier boundary. We compute the
    dollar amount that falls in each band and the commission earned in
    that band, then sum back to one row per (invoice × recruiter).

  STEP 4 — PAY DATE RESOLUTION via config_commission_relationship
    and form_payment. Priority order:
      1. form_payment.payment_received_date  (explicit receipt override)
      2. due_date + 120 days                 (FullBloom client hard rule)
      3. due_date + hold_days                (standard hold)
      4. payment_received_date               (hold_days = -1, pay on receipt)
      5. due_date                            (hold_days = 0, pay immediately)
      6. DATE '1900-01-01'                   (fallback / data-quality signal)

  What this model does NOT do
  ───────────────────────────
  - No bonus calculations (all in int_bonus)
  - No secondary recruiter payouts (in int_bonus)
*/

with invoice_detail as (

    select * from {{ ref('int_invoice_detail') }}

),

commission_config as (

    select * from {{ ref('stg_commission_form__config_commission') }}

),

commission_relationship as (

    select * from {{ ref('stg_commission_form__config_commission_relationship') }}

),

payment as (

    select * from {{ ref('stg_commission_form__form_payment') }}

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Aggregate multiple credit roles → one row per (invoice × recruiter)
-- ─────────────────────────────────────────────────────────────────────────────
aggregated as (

    select
        form_response_pk,
        job_order_number,
        client_name,
        candidate_name,
        invoice_amount,
        due_date,
        last_modified,
        recruiter_name,
        recruiter_email,
        is_valid_split,

        -- Sum credit % and $ across all roles this recruiter holds on this invoice
        sum(credit_percentage)                          as invoice_credit_percent,
        sum(credit_amount)                              as invoice_credit_amount,

        -- Concatenate role descriptions for the payout description column
        string_agg(split_description, ' | ')            as form_detail_description

    from invoice_detail
    -- Restrict to current calendar year so YTD totals and tier lookups
    -- reset on January 1st and never accumulate across years
    where extract(year from due_date) = extract(year from current_date())
    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Running YTD totals per recruiter, ordered by due_date
-- ─────────────────────────────────────────────────────────────────────────────
with_ytd as (

    select
        *,

        -- Recruiter's cumulative credited sales (used for tier band lookup)
        sum(invoice_credit_amount)
            over (
                partition by recruiter_email
                order by due_date asc
                rows between unbounded preceding and current row
            )                                           as total_commission_sales,

        -- Recruiter's cumulative gross invoices (displayed on report)
        sum(invoice_amount)
            over (
                partition by recruiter_email
                order by due_date asc
                rows between unbounded preceding and current row
            )                                           as total_invoice_ytd,

        -- Row number used as part of commission_pk (ensures uniqueness per recruiter)
        row_number()
            over (
                partition by recruiter_email
                order by due_date asc
            )                                           as commission_number

    from aggregated

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Tier calculation
-- Range join: every tier band this invoice's running total "touches" produces
-- one row. We then sum tier commissions back to one row per (invoice × recruiter).
-- ─────────────────────────────────────────────────────────────────────────────
tier_slices as (

    select
        with_ytd.form_response_pk,
        with_ytd.recruiter_email,

        commission_config.commission_tier,
        commission_config.commission_percentage         as tier_rate,

        -- How many dollars of this invoice fall within this tier band?
        least(with_ytd.total_commission_sales,          commission_config.higher_amount)
            - greatest(
                with_ytd.total_commission_sales - with_ytd.invoice_credit_amount,
                commission_config.lower_amount
              )                                         as tier_amount,

        -- Commission dollars earned in this tier for this invoice
        (
            least(with_ytd.total_commission_sales,      commission_config.higher_amount)
            - greatest(
                with_ytd.total_commission_sales - with_ytd.invoice_credit_amount,
                commission_config.lower_amount
              )
        ) * commission_config.commission_percentage     as tier_commission

    from with_ytd
    join commission_config
        on  with_ytd.recruiter_email
                = commission_config.employee_email
        -- Invoice's running total is inside or straddles this tier band
        and with_ytd.total_commission_sales
                >= commission_config.lower_amount
        and (with_ytd.total_commission_sales - with_ytd.invoice_credit_amount)
                <= commission_config.higher_amount

),

commission_summed as (

    select
        with_ytd.*,
        sum(cast(tier_slices.tier_commission as numeric))           as commission
    from with_ytd
    left join tier_slices
        on  with_ytd.form_response_pk   = tier_slices.form_response_pk
        and with_ytd.recruiter_email    = tier_slices.recruiter_email
    group by all

),

-- Point-in-time tier lookup: which band does total_commission_sales fall into
-- for this specific invoice? total_commission_sales is already locked by the
-- window function in with_ytd so this reflects the tier at the exact moment
-- this invoice was processed — it will not shift if future invoices push the
-- recruiter into a higher tier.
tier_at_time_of_invoice as (

    select
        commission_summed.form_response_pk,
        commission_summed.recruiter_email,
        commission_config.commission_percentage                     as current_tier_rate
    from commission_summed
    join commission_config
        on  commission_summed.recruiter_email           = commission_config.employee_email
        and commission_summed.total_commission_sales    >= commission_config.lower_amount
        and commission_summed.total_commission_sales    <  coalesce(
                commission_config.higher_amount + 0.01,
                commission_summed.total_commission_sales + 1
            )

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Resolve commission pay date
-- ─────────────────────────────────────────────────────────────────────────────
with_pay_date as (

    select
        commission_summed.*,
        tier_at_time_of_invoice.current_tier_rate,
        commission_relationship.config_commission_relationship_pk,
        commission_relationship.commission_hold_days,
        commission_relationship.secondary_recruiter_name,
        commission_relationship.secondary_recruiter_email,
        commission_relationship.commission_rate             as secondary_commission_rate,

        case
            -- 1. Explicit payment receipt overrides everything
            when payment.payment_received_date is not null
                then payment.payment_received_date

            -- 2. FullBloom always holds 120 days regardless of hold_days config
            when commission_relationship.commission_hold_days > 0
                and commission_summed.client_name = 'FullBloom'
                then date_add(commission_summed.due_date, interval 120 day)

            -- 3. Standard hold: add configured days to due date
            when commission_relationship.commission_hold_days > 0
                then date_add(
                        commission_summed.due_date,
                        interval commission_relationship.commission_hold_days day
                     )

            -- 4. hold_days = -1 means "pay when client pays us"
            when commission_relationship.commission_hold_days = -1
                then payment.payment_received_date

            -- 5. hold_days = 0 means pay on due date
            when commission_relationship.commission_hold_days = 0
                then commission_summed.due_date

            -- 6. Fallback — surface as a data-quality issue
            else date '1900-01-01'
        end                                                 as commission_pay_date,

        -- Capture actual payment receipt date for the "Date Rcd" report column
        payment.payment_received_date

    from commission_summed
    left join tier_at_time_of_invoice
        on  commission_summed.form_response_pk  = tier_at_time_of_invoice.form_response_pk
        and commission_summed.recruiter_email   = tier_at_time_of_invoice.recruiter_email
    left join commission_relationship
        on  commission_summed.recruiter_email
            = commission_relationship.primary_recruiter_email
    left join payment
        on  commission_summed.recruiter_name    = payment.recruiter_name
        and commission_summed.job_order_number  = payment.job_order_number

),

-- ─────────────────────────────────────────────────────────────────────────────
-- Final: generate surrogate PK, deduplicate
-- ─────────────────────────────────────────────────────────────────────────────
final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'form_response_pk',
            'recruiter_email',
            'commission_number'
        ]) }}               as commission_pk,
        *
    from with_pay_date
    -- If a recruiter appears in multiple relationship rows (data issue),
    -- keep the one with the longest hold (most conservative)
    qualify
        row_number() over (
            partition by form_response_pk, recruiter_email
            order by coalesce(commission_hold_days, 0) desc
        ) = 1

)

select * from final
/*
  int_commission
  ──────────────
  Grain  : one row per (invoice × recruiter).
  Sources: int_invoice_detail
           stg_commission_form__config_commission
           stg_commission_form__config_commission_relationship
           stg_commission_form__form_payment

  STEP 1 — AGGREGATE splits to one row per (invoice × recruiter).
  STEP 2 — RUNNING TOTALS per recruiter ordered by due_date.
  STEP 3 — TIER CALCULATION via range join to config_commission.
  STEP 4 — PAY DATE RESOLUTION via config_commission_relationship and form_payment.
            Priority: payment received > FullBloom 120d > hold days > pay on receipt > due date > fallback
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
        sum(credit_percentage)              as invoice_credit_percent,
        sum(credit_amount)                  as invoice_credit_amount,
        string_agg(split_description, ' | ') as form_detail_description
    from invoice_detail
    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Running YTD totals per recruiter, ordered by due_date
-- ─────────────────────────────────────────────────────────────────────────────
with_ytd as (

    select
        *,
        sum(invoice_credit_amount)
            over (
                partition by recruiter_email
                order by due_date asc
                rows between unbounded preceding and current row
            )                               as total_commission_sales,
        sum(invoice_amount)
            over (
                partition by recruiter_email
                order by due_date asc
                rows between unbounded preceding and current row
            )                               as total_invoice_ytd,
        row_number()
            over (
                partition by recruiter_email
                order by due_date asc
            )                               as commission_number
    from aggregated

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Tier calculation
-- ─────────────────────────────────────────────────────────────────────────────
tier_slices as (

    select
        with_ytd.form_response_pk,
        with_ytd.recruiter_email,
        commission_config.commission_tier,
        commission_config.commission_percentage                     as tier_rate,
        least(with_ytd.total_commission_sales, commission_config.higher_amount)
            - greatest(
                with_ytd.total_commission_sales - with_ytd.invoice_credit_amount,
                commission_config.lower_amount
              )                                                     as tier_amount,
        (
            least(with_ytd.total_commission_sales, commission_config.higher_amount)
            - greatest(
                with_ytd.total_commission_sales - with_ytd.invoice_credit_amount,
                commission_config.lower_amount
              )
        ) * commission_config.commission_percentage                 as tier_commission
    from with_ytd
    join commission_config
        on  with_ytd.recruiter_email            = commission_config.employee_email
        and with_ytd.total_commission_sales     >= commission_config.lower_amount
        and (with_ytd.total_commission_sales - with_ytd.invoice_credit_amount)
                                                <= commission_config.higher_amount

),

commission_summed as (

    select
        with_ytd.*,
        sum(cast(tier_slices.tier_commission as numeric))           as commission,
        -- Effective blended rate = commission earned / recruiter's credited amount.
        -- Uses safe_divide to handle edge case of zero credit amount.
        -- Correctly reflects blended rate when an invoice straddles a tier boundary.
        safe_divide(
            sum(cast(tier_slices.tier_commission as numeric)),
            with_ytd.invoice_credit_amount
        )                                                           as effective_commission_rate
    from with_ytd
    left join tier_slices
        on  with_ytd.form_response_pk   = tier_slices.form_response_pk
        and with_ytd.recruiter_email    = tier_slices.recruiter_email
    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Resolve commission pay date
-- ─────────────────────────────────────────────────────────────────────────────
with_pay_date as (

    select
        commission_summed.*,
        commission_relationship.config_commission_relationship_pk,
        commission_relationship.commission_hold_days,
        commission_relationship.secondary_recruiter_name,
        commission_relationship.secondary_recruiter_email,
        commission_relationship.commission_rate                     as secondary_commission_rate,
        case
            when payment.payment_received_date is not null
                then payment.payment_received_date
            when commission_relationship.commission_hold_days > 0
                and commission_summed.client_name = 'FullBloom'
                then date_add(commission_summed.due_date, interval 120 day)
            when commission_relationship.commission_hold_days > 0
                then date_add(
                        commission_summed.due_date,
                        interval commission_relationship.commission_hold_days day
                     )
            when commission_relationship.commission_hold_days = -1
                then payment.payment_received_date
            when commission_relationship.commission_hold_days = 0
                then commission_summed.due_date
            else date '1900-01-01'
        end                                                         as commission_pay_date,
        payment.payment_received_date
    from commission_summed
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
    qualify
        row_number() over (
            partition by form_response_pk, recruiter_email
            order by coalesce(commission_hold_days, 0) desc
        ) = 1

)

select * from final
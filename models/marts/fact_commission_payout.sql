/*
  fact_commission_payout
  ──────────────────────
  Grain  : one row per payout event (commission or bonus) per recruiter.
  Sources: int_payout, dim_date
*/

with payout as (

    select * from {{ ref('int_payout') }}

),

dim_date as (

    select
        date_day,
        next_pay_date
    from {{ ref('dim_date') }}

),

final as (

    select
        -- Keys
        payout.payout_pk,
        payout.payout_type,
        payout.bonus_type,
        payout.form_response_fk,

        -- Report columns
        payout.due_date                                             as date,
        payout.job_order_number,
        payout.client_name,
        payout.candidate_name,
        payout.recruiter_name,
        payout.due_date,
        payout.invoice_amount,

        -- Commissionable sales = recruiter credit % x invoice amount
        payout.invoice_credit_percent * payout.invoice_amount      as commissionable_sales,

        payout.total_commission_sales,

        -- Comm % = tier rate from config_commission at the time this invoice posted
        payout.current_tier_rate                                    as comm_percent,

        payout.commission_amount,
        payout.bonus_amount,
        payout.payment_received_date                                as date_received,
        payout.commission_pay_date                                  as guarantee_period_end,
        dim_date.next_pay_date                                      as date_paid,
        payout.payout_description                                   as notes,

        -- Data quality
        payout.is_valid_split,
        payout.last_modified

    from payout
    left join dim_date
        on dim_date.date_day = payout.commission_pay_date

)

select * from final
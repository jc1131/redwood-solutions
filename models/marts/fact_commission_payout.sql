/*
  fact_commission_payout
  ──────────────────────
  Grain  : one row per payout event (commission or bonus) per recruiter.
  Sources: int_payout, dim_date

  Column order mirrors the 2026 Commission Report:
    Date | Job Order # | Company | Candidate Name | Recruiter |
    Due Date | Invoice Amt | Deal Split % | Commissionable Sales |
    Total Sales YTD | Comm % | Commissions | Bonus | Date Rcd |
    Guarantee Period End | Date Pd | Notes

  Math check:
    invoice_amount × deal_split_percent   = commissionable_sales
    commissionable_sales × comm_percent   = commission_amount
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
        payout.due_date                                         as date,
        payout.job_order_number,
        payout.client_name,
        payout.candidate_name,
        payout.recruiter_name,
        payout.due_date,
        payout.invoice_amount,

        -- "Deal Split %" — recruiter's credited share of this invoice
        payout.invoice_credit_percent                           as deal_split_percent,

        -- "Commissionable Sales" — invoice_amount × deal_split_percent
        payout.invoice_credit_percent * payout.invoice_amount  as commissionable_sales,

        payout.total_commission_sales,

        -- "Comm %" — tier rate from config_commission based on current-year YTD sales
        -- commissionable_sales × comm_percent = commission_amount
        payout.current_tier_rate                                as comm_percent,

        payout.commission_amount,
        payout.bonus_amount,
        payout.payment_received_date                            as date_received,
        payout.commission_pay_date                              as guarantee_period_end,
        dim_date.next_pay_date                                  as date_paid,
        payout.payout_description                               as notes,

        -- Data quality
        payout.is_valid_split,
        payout.last_modified

    from payout
    left join dim_date
        on dim_date.date_day = payout.commission_pay_date

)

select * from final
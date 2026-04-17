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
        payout.signature_date                                  as date,
        payout.job_order_number,
        payout.client_name,
        payout.candidate_name,
        payout.recruiter_name,
        payout.signature_date                                  as due_date,
        payout.invoice_amount,
        payout.deal_split_percent,
        payout.invoice_amount * payout.deal_split_percent      as commissionable_sales,
        sum(payout.invoice_amount * payout.deal_split_percent)
            over (partition by payout.recruiter_name
                  order by payout.signature_date)              as total_sales_ytd,
        payout.comm_percent,
        payout.invoice_amount * payout.deal_split_percent *
        payout.comm_percent                                    as commission_amount,
        payout.bonus_amount                                    as bonus,
        payout.date_received,
        dateadd(day, payout.guarantee_period_days, payout.start_date)
                                                              as guarantee_period_end,
        payout.date_paid,
        payout.notes
    from payout
)

select * from final
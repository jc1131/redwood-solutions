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

    select * from {{ ref('int_commission') }}

)

select * from payout
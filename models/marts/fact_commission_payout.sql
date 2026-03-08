/*
  fact_commission_payout
  ──────────────────────
  Grain  : one row per payout event (commission or bonus) per recruiter.
  Sources: int_payout   (all commissions + bonuses unified)
           dim_date     (resolves next_pay_date from commission_pay_date)

  This mart's column order mirrors the 2026 Commission Report output:
    Date | Job Order # | Inv# | Company | Candidate Name | Due Date |
    Invoice Amt or Split | Commissionable Sales | Total Sales YTD |
    Comm % | Commissions | Bonus | Date Rcd |
    3 or 4 mo Guarantee Period | Date Pd | Notes

  All business logic lives upstream. This model only:
  - Joins int_payout to dim_date for the next payroll run date
  - Aliases columns to report-friendly names
  - Adds a calculated "guarantee_period_end" column for the
    "3 or 4 mo Guarantee Period" report column
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
        -- ── Keys ──────────────────────────────────────────────────────────
        payout.payout_pk,
        payout.payout_type,
        payout.bonus_type,
        payout.form_response_fk,

        -- ── Report columns ────────────────────────────────────────────────

        -- "Date" — the invoice due date (null for bonus rows)
        payout.due_date                                         as date,

        -- "Job Order #" and "Inv#" (same value, two report columns)
        payout.job_order_number,

        -- "Company"
        payout.client_name,

        -- "Candidate Name"
        payout.candidate_name,

        -- "Recruiter" — who receives this payout
        payout.recruiter_name,

        -- "Due Date" — invoice payment due date
        payout.due_date,

        -- "Invoice Amt or Split" — full invoice amount for commission rows
        payout.invoice_amount,

        -- "Commissionable Sales" — recruiter's credited portion of the invoice
        payout.invoice_credit_percent * payout.invoice_amount  as commissionable_sales,

        -- "Total Sales YTD"
        payout.total_commission_sales,

        -- "Comm %" — the blended tier rate for this invoice
        payout.invoice_credit_percent                           as comm_percent,

        -- "Commissions" — commission dollar amount
        payout.commission_amount,

        -- "Bonus" — bonus dollar amount
        payout.bonus_amount,

        -- "Date Rcd" — actual payment receipt date
        payout.payment_received_date                            as date_received,

        -- "3 or 4 mo Guarantee Period" — the commission pay date resolved by
        --  hold-day rules (i.e. the end of the guarantee/hold window)
        payout.commission_pay_date                              as guarantee_period_end,

        -- "Date Pd" — next payroll run date on or after the commission pay date
        dim_date.next_pay_date                                  as date_paid,

        -- "Notes" — payout description for any additional context
        payout.payout_description                               as notes,

        -- ── Data quality ──────────────────────────────────────────────────
        payout.is_valid_split,
        payout.last_modified

    from payout
    left join dim_date
        on dim_date.date_day = payout.commission_pay_date

)

select * from final
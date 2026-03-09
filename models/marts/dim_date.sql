/*
  dim_date
  ────────
  Grain  : one row per calendar date (2024-01-01 → 2026-12-31).
  Sources: dbt_date package macros.

  Enriched with:
  - Fiscal period columns via dbt_date.get_fiscal_periods
  - Semi-monthly pay cycle columns used by int_bonus (activity pay dates)
    and fact_draw_payout (draw netting)

  Pay cycle logic
  ───────────────
  The company runs a semi-monthly payroll: two pay dates per calendar month.
    Pay date 1 : the 14th (covers the 1st–15th of the month)
    Pay date 2 : last day of the month (covers the 16th–month end)

  pay_date        = the pay date this calendar date falls within
  pay_cycle_start = first day of this date's pay cycle period
  pay_cycle_end   = last day of this date's pay cycle period (= pay_date)
  next_pay_date   = the NEXT upcoming pay date from this calendar date
*/

{{
    config(
        materialized = 'table'
    )
}}

with date_dimension as (
    {{ dbt_date.get_date_dimension('2024-01-01', '2026-12-31') }}
),

fiscal_periods as (
    {{ dbt_date.get_fiscal_periods('date_dimension', year_end_month=1, week_start_day=1, shift_year=1) }}
),

final as (

    select
        d.*,

        -- Fiscal enrichment
        f.fiscal_week_of_year,
        f.fiscal_week_of_period,
        f.fiscal_period_number,
        f.fiscal_quarter_number,
        f.fiscal_period_of_quarter,

        -- ── Semi-monthly pay cycle columns ──────────────────────────────

        -- Which pay date does this calendar date belong to?
        cast(
            case
                when extract(day from d.date_day) <= 15
                    then date_trunc(d.date_day, month) + interval 14 day
                else last_day(d.date_day)
            end
        as date)                                    as pay_date,

        -- Start of this pay cycle period
        cast(
            case
                when extract(day from d.date_day) <= 15
                    then date_trunc(d.date_day, month)
                else date_trunc(d.date_day, month) + interval 15 day
            end
        as date)                                    as pay_cycle_start,

        -- End of this pay cycle period (same as pay_date)
        cast(
            case
                when extract(day from d.date_day) <= 15
                    then date_trunc(d.date_day, month) + interval 14 day
                else last_day(d.date_day)
            end
        as date)                                    as pay_cycle_end,

        -- Next upcoming pay date from this calendar date
        cast(
            case
                when extract(day from d.date_day) < 15
                    then date_trunc(d.date_day, month) + interval 14 day
                when extract(day from d.date_day) < extract(day from last_day(d.date_day))
                    then last_day(d.date_day)
                else date_trunc(d.date_day + interval 1 month, month) + interval 14 day
            end
        as date)                                    as next_pay_date

    from date_dimension d
    left join fiscal_periods f
        on d.date_day = f.date_day

)

select * from final
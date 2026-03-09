/*
  fact_draw_payout
  ────────────────
  Grain  : one row per (employee × semi-monthly pay period).
  Sources: stg_commission_form__config_draw
           int_commission  (commission earned, mapped to pay periods)
           dim_date        (semi-monthly pay period calendar)

  Business rules
  ──────────────
  1. Balance forward row — appears on draw_start_date (1st of year) with no
     Gross Amt Pd Out. Establishes opening debt from prior year. The balance
     forward amount is NOT amt_pd_back — it is money still owed.

  2. Draw is paid each semi-monthly period ONLY if running balance owed is
     below draw_max cap. If commission covers the employee that period, no
     draw is issued.

  3. Cap logic — balance forward + draws issued - commission received cannot
     exceed draw_max. Partial draws issued when approaching cap. Draw resumes
     if commission brings balance back below cap.

  4. Balance Owed = running (draws + balance forward) - commission received.
     Positive = owes money back. Negative = earned more than drawn.

  5. Amt Pd Back = commission received that period only. Does NOT include
     balance forward (that is still owed).
*/

with config_draw as (

    select * from {{ ref('stg_commission_form__config_draw') }}

),

commission_raw as (

    select
        recruiter_name      as employee_name,
        commission_pay_date,
        commission          as commission_amount
    from {{ ref('int_commission') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}
    where
        year_number     = extract(year from current_date())
        and date_day    <= current_date()

),

commission_by_period as (

    select
        commission_raw.employee_name,
        dim_date.pay_date,
        sum(commission_raw.commission_amount)   as commission_amount
    from commission_raw
    inner join dim_date
        on commission_raw.commission_pay_date = dim_date.date_day
    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Period spine — balance forward row + draw period rows
-- ─────────────────────────────────────────────────────────────────────────────
period_spine as (

    -- (a) Balance forward — prior year debt, flows into balance_owed as a debit,
    --     shows as $0 in gross_amt_pd_out output column
    select
        config_draw.config_draw_pk,
        config_draw.employee_name,
        config_draw.draw_start_date                             as pay_date,
        config_draw.draw_max,
        coalesce(config_draw.balance_owed, 0)                   as gross_amt_pd_out_raw,
        cast(0 as numeric)                                      as amt_pd_back_raw,
        'Balance forward from ' || cast(
            extract(year from config_draw.draw_start_date) - 1 as string
        )                                                       as notes,
        0                                                       as sort_order

    from config_draw
    where coalesce(config_draw.balance_owed, 0) != 0

    union all

    -- (b) Draw period rows
    select
        config_draw.config_draw_pk,
        config_draw.employee_name,
        pay_periods.pay_date,
        config_draw.draw_max,
        cast(config_draw.draw_amount as numeric)                as gross_amt_pd_out_raw,
        cast(0 as numeric)                                      as amt_pd_back_raw,
        'Semi-monthly draw $' || cast(config_draw.draw_amount as string) as notes,
        1                                                       as sort_order

    from config_draw
    cross join (select distinct pay_date from dim_date) pay_periods
    where pay_periods.pay_date >= config_draw.draw_start_date

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Join commission to draw period rows only
-- ─────────────────────────────────────────────────────────────────────────────
with_commission as (

    select
        period_spine.config_draw_pk,
        period_spine.employee_name,
        period_spine.pay_date,
        period_spine.draw_max,
        period_spine.notes,
        period_spine.sort_order,
        period_spine.gross_amt_pd_out_raw,

        -- Commission only applies to draw period rows (sort_order = 1)
        -- Balance forward row has no amt_pd_back — it is still owed
        case
            when period_spine.sort_order = 1
                then coalesce(commission_by_period.commission_amount, 0)
            else cast(0 as numeric)
        end                                                     as amt_pd_back

    from period_spine
    left join commission_by_period
        on  period_spine.employee_name  = commission_by_period.employee_name
        and period_spine.pay_date       = commission_by_period.pay_date
        and period_spine.sort_order     = 1

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Apply draw_max cap
-- ─────────────────────────────────────────────────────────────────────────────
with_cap as (

    select
        *,

        coalesce(
            sum(gross_amt_pd_out_raw - amt_pd_back) over (
                partition by employee_name
                order by pay_date asc, sort_order asc
                rows between unbounded preceding and 1 preceding
            ),
            0
        )                                                       as balance_before,

        case
            when sort_order = 0
                then gross_amt_pd_out_raw   -- balance forward flows into running total as debt
            when coalesce(
                    sum(gross_amt_pd_out_raw - amt_pd_back) over (
                        partition by employee_name
                        order by pay_date asc, sort_order asc
                        rows between unbounded preceding and 1 preceding
                    ), 0
                 ) >= draw_max
                then 0                      -- cap reached, no draw
            when coalesce(
                    sum(gross_amt_pd_out_raw - amt_pd_back) over (
                        partition by employee_name
                        order by pay_date asc, sort_order asc
                        rows between unbounded preceding and 1 preceding
                    ), 0
                 ) + gross_amt_pd_out_raw > draw_max
                then draw_max - coalesce(
                        sum(gross_amt_pd_out_raw - amt_pd_back) over (
                            partition by employee_name
                            order by pay_date asc, sort_order asc
                            rows between unbounded preceding and 1 preceding
                        ), 0
                     )                      -- partial draw up to cap
            else gross_amt_pd_out_raw       -- full draw
        end                                                     as gross_amt_pd_out_capped

    from with_commission

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['config_draw_pk', 'pay_date', 'sort_order']) }}
                                                as draw_pk,
        employee_name                           as recruiter_name,
        pay_date,

        -- Balance forward row shows $0 in gross_amt_pd_out — debt is in balance_owed only
        case
            when sort_order = 0 then cast(0 as numeric)
            else gross_amt_pd_out_capped
        end                                                     as gross_amt_pd_out,

        amt_pd_back,

        -- Positive = owes money back. Negative = earned more than drawn.
        sum(gross_amt_pd_out_capped - amt_pd_back) over (
            partition by employee_name
            order by pay_date asc, sort_order asc
            rows between unbounded preceding and current row
        )                                                       as balance_owed,

        draw_max                                as draw_cap,
        notes

    from with_cap

)

select * from final
/*
  fact_draw_payout
  ────────────────
  Grain  : one row per (employee × semi-monthly pay period).
  Sources: stg_commission_form__config_draw
           int_commission     (commission earned per pay period)
           dim_date           (pay period calendar)

  Models the semi-monthly draw-versus-commission netting for employees
  on a draw plan. For each pay period:
    - The configured draw amount is debited (negative)
    - Any commission that falls within that pay period is credited (positive)
    - A running balance tracks the cumulative draw owed

  The balance_amount column drives whether an employee owes money back
  (negative balance) or has earned above their draw (positive balance).

  Separate from fact_commission_payout because the grain (pay period)
  is different from the commission/bonus grain (invoice / bonus event).
*/

with config_draw as (

    select * from {{ ref('stg_commission_form__config_draw') }}

),

commission as (

    select
        recruiter_name          as employee_name,
        commission_pay_date,
        commission              as commission_amount
    from {{ ref('int_commission') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}
    where
        year_number = extract(year from current_date())
        and date_day <= current_date()

),

-- One row per distinct pay date in scope
pay_periods as (

    select distinct pay_date
    from dim_date

),

-- ─────────────────────────────────────────────────────────────────────────────
-- Align commissions to their pay period pay_date
-- ─────────────────────────────────────────────────────────────────────────────
commission_by_period as (

    select
        config_draw.config_draw_pk,
        commission.employee_name,
        dim_date.pay_date,
        sum(commission.commission_amount)       as commission_amount
    from commission
    left join dim_date
        on commission.commission_pay_date = dim_date.date_day
    inner join config_draw
        on commission.employee_name = config_draw.employee_name
    group by all

),

-- ─────────────────────────────────────────────────────────────────────────────
-- Generate one draw row per employee per pay period (since draw_start_date)
-- plus one "balance forward" row for any opening balance owed
-- ─────────────────────────────────────────────────────────────────────────────
draw_by_period as (

    -- Recurring semi-monthly draw deduction
    select
        config_draw.config_draw_pk,
        config_draw.employee_name,
        pay_periods.pay_date,
        config_draw.draw_amount * -1            as draw_amount,
        'Semi-monthly draw'                     as draw_description
    from config_draw
    cross join pay_periods
    where pay_periods.pay_date >= config_draw.draw_start_date

    union all

    -- Opening balance owed (carried forward from prior period)
    select
        config_draw.config_draw_pk,
        config_draw.employee_name,
        config_draw.draw_start_date             as pay_date,
        config_draw.balance_owed * -1           as draw_amount,
        'Balance forward'                       as draw_description
    from config_draw
    where config_draw.balance_owed != 0

),

-- ─────────────────────────────────────────────────────────────────────────────
-- Combine draw and commission per period, compute running balance
-- ─────────────────────────────────────────────────────────────────────────────
pay_period_net as (

    select
        draw_by_period.config_draw_pk,
        draw_by_period.employee_name,
        draw_by_period.pay_date,
        draw_by_period.draw_amount,
        coalesce(commission_by_period.commission_amount, 0)     as commission_amount,
        draw_by_period.draw_description,

        -- Running cumulative balance (negative = still owes draw back)
        sum(
            draw_by_period.draw_amount
            + coalesce(commission_by_period.commission_amount, 0)
        ) over (
            partition by draw_by_period.employee_name
            order by draw_by_period.pay_date
            rows between unbounded preceding and current row
        )                                                       as balance_amount

    from draw_by_period
    left join commission_by_period
        on  draw_by_period.pay_date         = commission_by_period.pay_date
        and draw_by_period.employee_name    = commission_by_period.employee_name

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['config_draw_pk', 'pay_date']) }}
            as draw_pk,
        employee_name   as recruiter_name,
        pay_date        as due_date,
        draw_amount,
        commission_amount,
        balance_amount,
        draw_description
    from pay_period_net

)

select * from final
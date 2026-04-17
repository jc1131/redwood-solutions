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

    -- (a) Balance forward — prior year debt carried in as opening debit
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
-- STEP 3: Apply draw_max cap using running balance before current row
-- ─────────────────────────────────────────────────────────────────────────────
with_cap as (

    select
        *,

        -- Running balance BEFORE this row (floored at 0).
        -- Represents how much draw debt is still outstanding entering this period.
        greatest(
            coalesce(
                sum(gross_amt_pd_out_raw - amt_pd_back) over (
                    partition by employee_name
                    order by pay_date asc, sort_order asc
                    rows between unbounded preceding and 1 preceding
                ),
                0
            ),
            0
        )                                                       as balance_before,

        -- Cap the draw: if already at/over draw_max, pay $0; if partially over, pay the remainder
        case
            when sort_order = 0
                then gross_amt_pd_out_raw   -- balance forward always flows in full
            when greatest(
                    coalesce(
                        sum(gross_amt_pd_out_raw - amt_pd_back) over (
                            partition by employee_name
                            order by pay_date asc, sort_order asc
                            rows between unbounded preceding and 1 preceding
                        ),
                        0
                    ),
                    0
                 ) >= draw_max
                then cast(0 as numeric)     -- cap reached, no draw paid
            when greatest(
                    coalesce(
                        sum(gross_amt_pd_out_raw - amt_pd_back) over (
                            partition by employee_name
                            order by pay_date asc, sort_order asc
                            rows between unbounded preceding and 1 preceding
                        ),
                        0
                    ),
                    0
                 ) + gross_amt_pd_out_raw > draw_max
                then draw_max - greatest(
                        coalesce(
                            sum(gross_amt_pd_out_raw - amt_pd_back) over (
                                partition by employee_name
                                order by pay_date asc, sort_order asc
                                rows between unbounded preceding and 1 preceding
                            ),
                            0
                        ),
                        0
                     )                      -- partial draw up to cap
            else gross_amt_pd_out_raw       -- full draw
        end                                                     as gross_amt_pd_out_capped

    from with_commission

),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Compute balance_owed after each row, then derive commission splits
-- ─────────────────────────────────────────────────────────────────────────────
with_balance as (

    select
        *,

        -- Running balance AFTER this row: cumulative (draw - commission), floored at 0.
        -- Floored because commission overpayment doesn't carry as a credit — it pays out.
        greatest(
            sum(gross_amt_pd_out_capped - amt_pd_back) over (
                partition by employee_name
                order by pay_date asc, sort_order asc
                rows between unbounded preceding and current row
            ),
            0
        )                                                       as balance_owed_running

    from with_cap

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['config_draw_pk', 'pay_date', 'sort_order']) }}
                                                as draw_pk,
        employee_name                           as recruiter_name,
        pay_date,

        -- Fixed draw paid out this period. Balance forward row shows $0 here —
        -- that debt already lives in balance_owed via the opening row.
        case
            when sort_order = 0 then cast(0 as numeric)
            else gross_amt_pd_out_capped
        end                                                     as gross_amt_pd_out,

        -- Total commission earned this period (raw amt paid back)
        amt_pd_back,

        -- Portion of commission used to pay down the outstanding draw balance.
        -- = however much of amt_pd_back was consumed closing the gap toward 0.
        -- When there is no prior balance, this is 0.
        case
            when sort_order = 1 and amt_pd_back > 0
                -- balance_before is the debt entering this row (always >= 0)
                -- amt_pd_back first chips away at balance_before; anything left pays out
                then least(amt_pd_back, balance_before + gross_amt_pd_out_capped)
            else cast(0 as numeric)
        end                                                     as commission_applied_to_balance,

        -- Portion of commission left after clearing the balance — paid to the employee.
        -- = amt_pd_back minus whatever was applied to the balance; floored at 0.
        case
            when sort_order = 1 and amt_pd_back > 0
                then greatest(amt_pd_back - (balance_before + gross_amt_pd_out_capped), 0)
            else cast(0 as numeric)
        end                                                     as commission_amt_paid_out,

        -- Outstanding draw balance after this period. Floored at 0 —
        -- overpaid commission does not create a negative (credit) balance.
        balance_owed_running                                    as balance_owed,

        draw_max                                as draw_cap,
        notes

    from with_balance

)

select * from final
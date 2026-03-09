/*
  int_bonus
  ---------
  Grain  : one row per bonus payout event per recruiter.
  Sources: stg_commission_form__config_bonus      (threshold / IT bonuses)
           stg_commission_form__form_activity      (monthly activity bonuses)
           int_commission                          (secondary split bonuses + sales totals)
           dim_date                                (activity bonus pay date lookup)

  Three bonus types live as named CTEs and are UNION ALL'd at the bottom.

  Output schema (all types normalised to these columns)
  bonus_pk, recruiter_name, bonus_pay_date, bonus_amount, bonus_description, bonus_type
*/

with commission as (

    select * from {{ ref('int_commission') }}

),

config_bonus as (

    select * from {{ ref('stg_commission_form__config_bonus') }}

),

form_activity as (

    select * from {{ ref('stg_commission_form__form_activity') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

-- -----------------------------------------------------------------------------
-- TYPE 1: Sales threshold and IT assistance bonuses
-- -----------------------------------------------------------------------------

sales_bonus_plans as (

    select * from config_bonus
    where bonus_plan in ('Annual Sales Goal', 'Quarter Sales Goal')

),

it_bonus_plans as (

    select * from config_bonus
    where bonus_plan = 'Quarter IT Assistance'

),

sales_bonus_agg as (

    select
        b.config_bonus_pk,
        b.employee_name,
        b.bonus_plan,
        b.bonus_start_date,
        b.bonus_end_date,
        b.bonus_threshold,
        b.bonus_amount,
        sum(c.invoice_credit_amount)    as actual_sales

    from sales_bonus_plans b
    join commission c
        on  c.recruiter_email       = b.employee_email
        and c.due_date between b.bonus_start_date and b.bonus_end_date
    group by all

),

sales_bonuses as (

    select
        config_bonus_pk                                             as bonus_pk,
        employee_name                                               as recruiter_name,
        bonus_end_date                                              as bonus_pay_date,
        case
            when actual_sales >= bonus_threshold then bonus_amount
            else 0
        end                                                         as bonus_amount,
        concat(
            employee_name, chr(39), 's ', bonus_plan,
            ' bonus: threshold $', cast(bonus_threshold as string), ' - ',
            case
                when actual_sales >= bonus_threshold
                    then 'met - payout approved'
                else 'not yet met'
            end
        )                                                           as bonus_description,
        'sales_threshold'                                           as bonus_type

    from sales_bonus_agg
    where actual_sales >= bonus_threshold

),

it_bonuses as (

    select
        config_bonus_pk                                             as bonus_pk,
        employee_name                                               as recruiter_name,
        bonus_end_date                                              as bonus_pay_date,
        bonus_amount,
        'Quarterly IT Assistance bonus'                             as bonus_description,
        'sales_threshold'                                           as bonus_type

    from it_bonus_plans
    where bonus_end_date < current_date()

),

-- -----------------------------------------------------------------------------
-- TYPE 2: Monthly activity bonuses
-- -----------------------------------------------------------------------------

activity_pay_dates as (

    select
        month_name,
        month_end_date,
        next_pay_date
    from dim_date
    where year_number = extract(year from current_date())
    qualify
        row_number() over (
            partition by month_name, month_end_date
            order by next_date_day desc
        ) = 1

),

activity_bonuses as (

    select
        {{ dbt_utils.generate_surrogate_key(['fa.form_activity_pk']) }}
                                                                    as bonus_pk,
        fa.activity_bonus_recipient                                 as recruiter_name,
        apd.next_pay_date                                           as bonus_pay_date,
        500.00                                                      as bonus_amount,
        concat(apd.month_name, ' activity bonus - $500')            as bonus_description,
        'activity'                                                  as bonus_type

    from form_activity fa
    left join activity_pay_dates apd
        on apd.month_name = fa.activity_bonus_month

),

-- -----------------------------------------------------------------------------
-- TYPE 3: Secondary recruiter split bonuses
-- -----------------------------------------------------------------------------

secondary_bonuses as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'config_commission_relationship_pk',
            'form_response_pk'
        ]) }}                                                       as bonus_pk,
        secondary_recruiter_name                                    as recruiter_name,
        commission_pay_date                                         as bonus_pay_date,
        round(invoice_credit_amount * secondary_commission_rate, 2) as bonus_amount,
        concat(
            'Secondary split - Job Order #',
            cast(job_order_number as string),
            ' due ', format_date('%Y-%m-%d', due_date),
            ', paid ', format_date('%Y-%m-%d', commission_pay_date)
        )                                                           as bonus_description,
        'secondary_split'                                           as bonus_type

    from commission
    where secondary_recruiter_email is not null

),

-- -----------------------------------------------------------------------------
-- UNION all three types
-- -----------------------------------------------------------------------------

final as (

    select * from sales_bonuses
    union all
    select * from it_bonuses
    union all
    select * from activity_bonuses
    union all
    select * from secondary_bonuses

)

select * from final
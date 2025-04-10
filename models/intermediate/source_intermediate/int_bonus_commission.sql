WITH base_commission AS (
    SELECT * FROM {{ ref('int_commission') }}
), base_bonus as (
    SELECT * FROM {{ ref('stg_commission_form__config_bonus') }}
),
bonus_it AS (
    SELECT * FROM base_bonus where bonus_plan in ('Quarter IT Assistance')
), 
bonus_configuration AS (
    SELECT * FROM base_bonus where bonus_plan in ('Annual Sales Goal', 'Quarter Sales Goal')
), bonus_agg AS (
    SELECT 
        b.employee_name,
        b.config_bonus_pk,
        b.bonus_plan,
        b.bonus_start_date,
        b.bonus_end_date,
        b.bonus_threshold,
        b.bonus_amount,
        SUM(s.invoice_credit_amount) AS total_sales
    FROM base_commission s
    JOIN bonus_configuration b 
        ON s.recruiter_email = b.employee_email
        AND s.due_date BETWEEN b.bonus_start_date AND b.bonus_end_date
    GROUP BY 
        all
), sale_bonus as (

SELECT 
    employee_name,
    config_bonus_pk,
    bonus_plan,
    bonus_start_date,
    bonus_end_date,
    bonus_threshold,
    bonus_amount,
    total_sales,
    CASE 
        WHEN total_sales >= bonus_threshold THEN bonus_amount
        ELSE 0
    END AS payout_amount,
     FORMAT(
        '%s’s %s bonus plan threshold of $%s has %s reached.',
        employee_name,
        bonus_plan,
        CAST(bonus_threshold AS STRING),
        CASE 
            WHEN total_sales >= bonus_threshold THEN 'been'
            ELSE 'not been'
        END
    ) AS bonus_description
FROM bonus_agg
),it_bonus as (
    select 
employee_name,
    config_bonus_pk,
    bonus_plan,
    bonus_start_date,
    bonus_end_date,
    null bonus_threshold,
    bonus_amount,
    null as total_sales,
    bonus_amount as payout_amount,
    'quarterly bonus for IT Assistance' AS bonus_description

 from bonus_it
where bonus_end_date < current_date()
),

final as (
    select
        *
    from sale_bonus
    where payout_amount > 0

    union all

    select
    *
    from it_bonus


)
select * from final


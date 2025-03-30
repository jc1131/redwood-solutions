WITH base_commission AS (
    SELECT * FROM {{ ref('int_commission') }}
),bonus_configuration AS (
    SELECT * FROM {{ ref('stg_commission_form__config_bonus') }}
),
combine_bonus_sale AS (
    SELECT
        config_bonus_pk,
        bonus_configuration.employee_email AS recruiter_email,
        bonus_configuration.employee_name AS recruiter_name,
        bonus_configuration.bonus_plan,
        bonus_configuration.bonus_threshold,
        bonus_configuration.bonus_amount,
        bonus_configuration.bonus_start_date,
        bonus_configuration.bonus_end_date,
        base_commission.due_date,
        base_commission.total_commission_sales,

 

    FROM bonus_configuration
    INNER JOIN base_commission ON
        bonus_configuration.employee_email = base_commission.recruiter_email
        AND base_commission.due_date BETWEEN bonus_configuration.bonus_start_date AND bonus_configuration.bonus_end_date
)
    SELECT
        config_bonus_pk AS primary_bonus_pk,
        recruiter_name,
        CASE
            WHEN total_commission_sales >= bonus_threshold THEN due_date
            ELSE bonus_end_date
        END AS bonus_pay_date,
        CASE
            WHEN total_commission_sales >= bonus_threshold THEN bonus_amount
            ELSE 0
        END AS bonus_amount,
        CONCAT(bonus_plan, ' payout') AS bonus_description
    FROM combine_bonus_sale
where bonus_amount > 0
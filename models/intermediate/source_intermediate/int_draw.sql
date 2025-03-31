WITH base_commission AS (
    SELECT
        recruiter_name AS employee_name,
        commission_pay_date,
        commission AS commission_amount
    FROM {{ ref('int_commission') }}
),

config_draw AS (
    SELECT
        config_draw_pk,
        employee_name,
        draw_amount,
        draw_start_date,
        balance_owed
    FROM {{ ref('stg_commission_form__config_draw') }}
),

dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE
        year_number = EXTRACT(YEAR FROM CURRENT_DATE())
        AND date_day <= CURRENT_DATE()
),
dim_pay_date as (
    select distinct pay_date from dim_date
),

-- Align int_commission to nearest pay period
commission_pay_period AS (
    SELECT
        config_draw.config_draw_pk,
        base_commission.employee_name,
        dim_date.pay_date,
        SUM(base_commission.commission_amount) AS commission_amount
    FROM base_commission
    LEFT JOIN dim_date
        ON base_commission.commission_pay_date = dim_date.date_day
    INNER JOIN config_draw
        ON base_commission.employee_name = config_draw.employee_name
    GROUP BY config_draw.config_draw_pk, base_commission.employee_name, dim_date.pay_date
),draw_pay_period AS (
    SELECT
        config_draw.config_draw_pk,
        config_draw.employee_name,
        dim_pay_date.pay_date,
        config_draw.draw_amount * -1 AS draw_amount,
        'Semi-monthly draw' AS draw_description
    FROM config_draw
    CROSS JOIN dim_pay_date
    WHERE dim_pay_date.pay_date >= config_draw.draw_start_date

    UNION ALL

    SELECT
        config_draw.config_draw_pk,
        config_draw.employee_name,
        config_draw.draw_start_date AS pay_date,
        config_draw.balance_owed * -1 AS draw_amount,
        'Balance forward' AS draw_description
    FROM config_draw
),
-- Combine draws and commissions by pay period
pay_period_amount AS (
    SELECT
        draw_pay_period.config_draw_pk,
        draw_pay_period.employee_name,
        draw_pay_period.pay_date,
        draw_pay_period.draw_amount,
        COALESCE(commission_pay_period.commission_amount, 0) AS commission_amount,
        draw_pay_period.draw_description,
        SUM(
            draw_pay_period.draw_amount
            + COALESCE(commission_pay_period.commission_amount, 0)
        ) OVER (
            PARTITION BY draw_pay_period.employee_name
            ORDER BY draw_pay_period.pay_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_amount
    FROM draw_pay_period
    LEFT JOIN commission_pay_period
        ON draw_pay_period.pay_date = commission_pay_period.pay_date
        AND draw_pay_period.employee_name = commission_pay_period.employee_name
),

-- Generate surrogate key
pk_generation AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['config_draw_pk', 'pay_date']) }} AS draw_pk,
        *
    FROM pay_period_amount
)

SELECT * FROM pk_generation
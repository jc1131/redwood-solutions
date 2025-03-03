WITH base_commission AS (
    SELECT * FROM {{ ref('int_commission') }}
),

config_draw AS (
    SELECT * FROM {{ ref('stg_commission_form__config_draw') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

pay_period AS (
    SELECT DISTINCT pay_date
    FROM dim_date
    WHERE
        year_number = EXTRACT(YEAR FROM CURRENT_DATE())
        AND date_day <= CURRENT_DATE()

)
,
commission_pay_period AS (
    SELECT
        config_draw.config_draw_pk,
        recruiter_name AS employee_name,
        pay_date,
        SUM(tier_commission) AS commission_amount

    FROM base_commission
    INNER JOIN
        config_draw
        ON base_commission.recruiter_name = config_draw.employee_name
    LEFT JOIN dim_date ON base_commission.due_date = dim_date.date_day
    GROUP BY ALL
),

draw_pay_period AS (
    SELECT
        config_draw.config_draw_pk,
        employee_name,
        pay_period.pay_date,
        draw_amount * -1 AS draw_amount,
        'Semi-monthly draw' AS draw_description

    FROM config_draw
    INNER JOIN pay_period ON 1 = 1
    UNION ALL

    SELECT
        config_draw.config_draw_pk,
        employee_name,
        config_draw.draw_start_date,
        balance_owed * -1 AS draw_amount,
        'Balance forward' AS draw_description
    FROM config_draw
    INNER JOIN dim_date ON config_draw.draw_start_date = dim_date.date_day
),

pay_period_amount AS (

    SELECT
        draw_pay_period.config_draw_pk,
        draw_pay_period.employee_name,
        draw_pay_period.pay_date,
        draw_pay_period.draw_amount,
        commission_pay_period.commission_amount,
        draw_pay_period.draw_description,
        SUM(
            draw_pay_period.draw_amount
            + COALESCE(commission_pay_period.commission_amount, 0)
        )
            OVER (
                PARTITION BY draw_pay_period.employee_name
                ORDER BY draw_pay_period.pay_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS balance_amount
    FROM draw_pay_period
    LEFT JOIN commission_pay_period ON
        draw_pay_period.pay_date = commission_pay_period.pay_date
        AND draw_pay_period.employee_name = commission_pay_period.employee_name
),

pk_generation AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['config_draw_pk', 'pay_date']) }} AS draw_pk,
        *
    FROM pay_period_amount
)

SELECT * FROM pk_generation

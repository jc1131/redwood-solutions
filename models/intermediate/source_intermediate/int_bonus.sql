WITH base_response AS (
    SELECT * FROM {{ ref('int_response_combined') }}
),

base_activity as (
    select * from {{ ref('stg_commission_form__form_activity')}}
),

config_commission_relationship AS (
    SELECT *
    FROM {{ ref('stg_commission_form__config_commission_relationship') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

activity_bonus_date AS (
    SELECT
        month_name,
        month_end_date,
        next_pay_date
    FROM dim_date
    WHERE year_number = EXTRACT(YEAR FROM CURRENT_DATE())
    QUALIFY
        ROW_NUMBER()
            OVER (
                PARTITION BY month_name, month_end_date
                ORDER BY next_date_day DESC
            )
        = 1
    ORDER BY 2 ASC
),activity_bonus as (
    select
    base_activity.form_activity_pk
    ,base_activity.activity_bonus_recipient
    ,activity_bonus_date.next_pay_date as bonus_pay_date
    ,500 as bonus_amount
    ,CONCAT(activity_bonus_date.month_name, ' activity bonus') as bonus_description
    from base_activity
        left join activity_bonus_date on activity_bonus_date.month_name = base_activity.activity_bonus_month
),

bonus_configuration AS (
    SELECT * FROM {{ ref('stg_commission_form__config_bonus') }}
),

secondary_bonus AS (
    SELECT distinct 
        {{ dbt_utils.generate_surrogate_key(['config_commission_relationship.config_commission_relationship_pk', 'job_order_number']) }}
            AS secondary_bonus_pk,
        config_commission_relationship.secondary_recruiter_name
            AS recruiter_name,
        DATE_ADD(work_start_date, INTERVAL commission_hold_days DAY)
            AS bonus_pay_date,
        config_commission_relationship.commission_rate
        * credit_amount AS bonus_amount,
        CONCAT(
            FORMAT('%g', config_commission_relationship.commission_rate * 100),
            '% commission from ',
            config_commission_relationship.primary_recruiter_name,
            ' on Job Order #', job_order_number,
            ' (Start Date: ', work_start_date,
            ', Hold Days: ', commission_hold_days
        ) AS bonus_description
    FROM base_response
    JOIN
        config_commission_relationship
        ON
            base_response.recruiter_email
            = config_commission_relationship.primary_recruiter_email

    WHERE config_commission_relationship.secondary_recruiter_email IS NOT null

),

sale_ytd AS (
    SELECT
        form_response_combine_pk,
        due_date,
        recruiter_email,
        invoice_amount,
        SUM(invoice_amount)
            OVER (PARTITION BY recruiter_email ORDER BY due_date ASC)
            AS total_sales_ytd
    FROM base_response


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
        base_response.due_date,
        SUM(base_response.invoice_amount) OVER (
            PARTITION BY
                bonus_configuration.employee_email,
                bonus_configuration.bonus_start_date,
                bonus_configuration.bonus_end_date
            ORDER BY base_response.due_date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS bonus_ytd

    FROM bonus_configuration
    INNER JOIN base_response ON
        bonus_configuration.employee_email = base_response.recruiter_email
        AND base_response.due_date BETWEEN bonus_configuration.bonus_start_date AND bonus_configuration.bonus_end_date
),

primary_bonus AS (
    SELECT
        config_bonus_pk AS primary_bonus_pk,
        recruiter_name,
        CASE
            WHEN bonus_ytd >= bonus_threshold THEN due_date
            ELSE bonus_end_date
        END AS bonus_pay_date,
        CASE
            WHEN bonus_ytd >= bonus_threshold THEN bonus_amount
            ELSE 0
        END AS bonus_amount,
        CONCAT(bonus_plan, ' payout') AS bonus_description
    FROM combine_bonus_sale
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY config_bonus_pk
        ORDER BY bonus_pay_date ASC
    ) = 1
),
combine_bonus AS (
    SELECT * FROM secondary_bonus
    UNION ALL
    SELECT * FROM primary_bonus
    UNION ALL
    select * from activity_bonus
),final as (

    select
    secondary_bonus_pk as bonus_pk 
    ,recruiter_name
    ,bonus_pay_date
    ,bonus_amount
    ,bonus_description
    from combine_bonus
)

SELECT * FROM final
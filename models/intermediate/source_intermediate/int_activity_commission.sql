with base_activity as (
    select * from {{ ref('stg_commission_form__form_activity')}}
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
)
select * from activity_bonus
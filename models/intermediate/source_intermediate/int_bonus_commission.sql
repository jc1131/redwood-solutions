WITH base_commission AS (
    SELECT * FROM {{ ref('int_commission') }}
), bonus_configuration AS (
    SELECT * FROM {{ ref('stg_commission_form__config_bonus') }}
), bonus_agg AS (
    SELECT 
        s.recruiter_email,
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
), final as (

SELECT 
    recruiter_email,
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
    END AS payout_amount
FROM bonus_agg
)
select * from final
where payout_amount > 0
WITH base_response AS (
    SELECT * 
    FROM {{ ref('int_response_running_total') }}
),
commission_config AS (
    SELECT * 
    FROM {{ ref('stg_commission_form__config_commission') }}
),
commission_config_relationship as (
    select * from {{ ref('stg_commission_form__config_commission_relationship') }}
)
commission_tier AS (
    SELECT 
        base_response.form_response_pk,
        base_response.recruiter_email,
        base_response.due_date,
        base_response.invoice_amount,
        base_response.credit_amount,
        base_response.running_total,
        commission_config.commission_tier,
        commission_config.lower_amount,
        commission_config.higher_amount,
        commission_config.commission_percentage,
        -- Calculate the tier amount
        LEAST(base_response.running_total, commission_config.higher_amount) 
          - GREATEST(base_response.running_total - base_response.credit_amount, commission_config.lower_amount) AS tier_amount,
        -- Calculate the commission for this tier
        (LEAST(base_response.running_total, commission_config.higher_amount) 
          - GREATEST(base_response.running_total - base_response.credit_amount, commission_config.lower_amount)) 
          * commission_config.commission_percentage AS tier_commission
    FROM base_response
    JOIN commission_config 
      ON base_response.running_total >= commission_config.lower_amount 
      AND base_response.running_total - base_response.credit_amount <= commission_config.higher_amount
      AND base_response.recruiter_email = commission_config.employee_email
    WHERE recruiter_email = 'Gayle Simons@fieldpros.com'
), final as (
    SELECT 
    form_response_pk
    ,recruiter_email
    ,due_date
    ,invoice_amount
    ,credit_amount
    ,running_total
    ,SUM(cast(tier_commission as int)) AS total_commission
    ,'primary ' as commission_description
FROM commission_tier
GROUP BY all
)
select *
from final
  
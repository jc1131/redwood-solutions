WITH base_response AS (
    SELECT * FROM {{ ref('int_response_combined') }}
),
commission_config AS (
    SELECT * 
    FROM {{ ref('stg_commission_form__config_commission') }}
),
commission_tier AS (
    SELECT 
        base_response.form_response_combine_pk,
        ROW_NUMBER() OVER (partition by form_response_combine_pk ORDER BY form_response_combine_pk) AS commission_row_number,  -- Add row number here
        base_response.recruiter_name,
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
), final as (
    SELECT 
    {{ dbt_utils.generate_surrogate_key(['form_response_combine_pk', 'commission_row_number']) }} as form_commission_pk
    ,form_response_combine_pk as form_response_combine_fk
    ,recruiter_name as recruiter_name
    ,due_date
    ,invoice_amount
    ,credit_amount
    ,running_total
    ,lower_amount
    ,higher_amount
    ,commission_percentage
    ,tier_amount
    ,tier_commission
FROM commission_tier
)
select 
*
from final
  
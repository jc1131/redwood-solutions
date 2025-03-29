with base_commission as (
    select * from {{ ref('int_commission') }}
),
config_commission_relationship AS (
    SELECT *
    FROM {{ ref('stg_commission_form__config_commission_relationship') }}
)
select 
        {{ dbt_utils.generate_surrogate_key(['config_commission_relationship.config_commission_relationship_pk', 'base_commission.form_response_fk']) }}
            AS secondary_bonus_pk,
        base_commission.form_response_fk,
        config_commission_relationship.secondary_recruiter_name
            AS recruiter_name,
        config_commission_relationship.commission_rate
        * invoice_credit_amount AS bonus_amount,
        config_commission_relationship.commission_hold_days
    FROM base_commission
    JOIN
        config_commission_relationship
        ON
            base_commission.recruiter_email
            = config_commission_relationship.primary_recruiter_email

    WHERE config_commission_relationship.secondary_recruiter_email IS NOT null
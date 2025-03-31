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
        config_commission_relationship.secondary_recruiter_name
            AS recruiter_name,
        config_commission_relationship.commission_rate
        * invoice_credit_amount AS bonus_amount,
        commission_pay_date as bonus_pay_date,
        CONCAT(
        'Job Order #', CAST(job_order_number AS STRING), 
        ' was due on ', FORMAT_DATE('%Y-%m-%d', due_date),
        ', but the payout is now held for an additional ', commission_pay_date) as bonus_description
        
    FROM base_commission
    JOIN
        config_commission_relationship
        ON
            base_commission.recruiter_email
            = config_commission_relationship.primary_recruiter_email

    WHERE config_commission_relationship.secondary_recruiter_email IS NOT null
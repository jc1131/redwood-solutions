WITH secondary_bonus AS (
select * from {{ ref('int_secondary_commission') }}
)
,
primary_bonus as (
    select * from {{ ref('int_bonus_commission') }}
),
activity_bonus as (
    select * from {{ ref('int_activity_commission') }}
)
,combine_bonus AS (
    SELECT 
    secondary_bonus_pk
    ,recruiter_name
    ,bonus_amount
    ,bonus_pay_date
    ,bonus_description
    
     FROM secondary_bonus
    UNION ALL
    SELECT 
    config_bonus_pk
    ,employee_name
    ,payout_amount
    ,bonus_end_date
    ,bonus_description
    
    FROM primary_bonus
    UNION ALL
    select 
    form_activity_pk
    ,activity_bonus_recipient
    ,bonus_amount
    ,bonus_pay_date
    ,bonus_description
    
    
     from activity_bonus
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
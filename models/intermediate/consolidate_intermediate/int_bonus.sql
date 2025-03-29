WITH secondary_bonus AS (
select * from secondary_bonus
)
,
primary_bonus as (
    select * from primary_bonus
),
activity_bonus as (
    select * from activity_bonus
)
,combine_bonus AS (
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
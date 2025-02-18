with int_commission as (
    select * from {{ ref('int_commission') }}
), 
int_bonus as (
    select * from {{ ref('int_bonus') }}
),
combine_compensation as (

    SELECT
    commission_pk,
    form_response_combine_fk,
    recruiter_name as recruiter_name,
    due_date AS due_date,
    invoice_amount AS invoice_amount,
    SUM(invoice_amount) OVER (PARTITION BY recruiter_name ORDER BY due_date) AS total_invoice_ytd,
    commission_percentage AS commission_percentage,
    sum(tier_commission) AS commission_amount,
    NULL AS other_comm_and_bonus,
    'Commission Sale' as payout_description
    from int_commission
    group by all

    union all

    select
    bonus_pk 
    ,null as form_response_combine_fk
    ,recruiter_name
    ,bonus_pay_date as due_date
    ,null as invoice_amount
    ,null AS total_invoice_ytd
    ,null as commission_percentage
    ,null as commission_amount
    ,bonus_amount as other_comm_and_bonus
    ,bonus_description as payout_description
    from int_bonus
)

select * from combine_compensation
where recruiter_name = 'Daniel Burke'
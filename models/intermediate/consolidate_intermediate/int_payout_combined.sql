with int_commission as (
    select * from {{ ref('int_commission') }}
), 
int_bonus as (
    select * from {{ ref('int_bonus') }}
),
combine_compensation as (

    SELECT
    commission_pk,
    form_response_fk,
    recruiter_name as recruiter_name,
    due_date as invoice_payment_date,
    commission_pay_date as commission_pay_date,
    invoice_amount AS invoice_amount,
    total_commission_sales AS total_invoice_ytd,
    invoice_credit_percent AS commission_percentage,
    commission AS commission_amount,
    NULL AS other_comm_and_bonus,
    concat('Commission Sale: ',form_detail_description) as payout_description
    from int_commission

    union all

    select
    bonus_pk 
    ,null as form_response_combine_fk
    ,recruiter_name
    ,null as invoice_payment_date
    ,bonus_pay_date as commission_pay_date
    ,null as invoice_amount
    ,null AS total_invoice_ytd
    ,null as commission_percentage
    ,null as commission_amount
    ,bonus_amount as other_comm_and_bonus
    ,bonus_description as payout_description
    from int_bonus
)

select * from combine_compensation

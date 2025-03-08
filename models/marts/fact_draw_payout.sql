with int_draw as (
    select * from {{ ref('int_draw') }}
)
select

    draw_pk
    ,employee_name as recruiter_name
    ,pay_date as due_date
    ,draw_amount
    ,commission_amount
    ,balance_amount
    ,draw_description
from int_draw
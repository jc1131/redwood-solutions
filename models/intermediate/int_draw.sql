with base_commission as (
    select * from {{ ref('int_commission') }}
),
config_draw as (
    select * from {{ ref('stg_commission_form__config_draw') }}
),
dim_date as (
    select * from {{ ref('dim_date')}}
),
pay_period as (
    select distinct
    pay_date
    from dim_date
    WHERE year_number = EXTRACT(YEAR FROM CURRENT_DATE())
    and date_day <= CURRENT_DATE()

)
,
commission_pay_period as (
select 
    recruiter_name as employee_name
    ,pay_date
    ,sum(tier_commission) as commission_amount

from base_commission
    join config_draw on config_draw.employee_name = base_commission.recruiter_name
    left join dim_date on dim_date.date_day = base_commission.due_date
group by all
),draw_pay_period as (
select 
employee_name
,pay_period.pay_date
,draw_amount * - 1 as draw_amount
,'Semi-monthly draw' as draw_description

from config_draw
join pay_period on 1=1
union all 

select
employee_name
,config_draw.draw_start_date
,balance_owed * -1 as draw_amount
,'Balance forward' as draw_description
from config_draw
join dim_date on dim_date.date_day = config_draw.draw_start_date
)
,pay_period_amount as (

select
    draw_pay_period.employee_name
    ,draw_pay_period.pay_date
    ,draw_pay_period.draw_amount
    ,commission_pay_period.commission_amount
    , SUM(draw_pay_period.draw_amount + COALESCE(commission_pay_period.commission_amount, 0)) 
        OVER (PARTITION BY draw_pay_period.employee_name ORDER BY draw_pay_period.pay_date 
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as balance_amount
    ,draw_pay_period.draw_description
from draw_pay_period
    left join commission_pay_period on commission_pay_period.pay_date = draw_pay_period.pay_date
    and  commission_pay_period.employee_name = draw_pay_period.employee_name
 )
select * from pay_period_amount

with source_recruiter as (
    select * from {{ ref('stg_commission_form__config_commission') }}
),
rollup_recruiter as (
    select distinct
    employee_name as recruiter_name
    ,employee_email as recruiter_email
    from source_recruiter
)
select * from rollup_recruiter
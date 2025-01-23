with 

source as (

    select * from {{ source('commission_form', 'config_bonus') }}

),

renamed as (

    select
        employee_name,
        employee_email,
        bonus_plan,
        bonus_start_date,
        bonus_end_date,
        bonus_threshold,
        bonus_amount,
        effective_from

    from source

)

select * from renamed

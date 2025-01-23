with 

source as (

    select * from {{ source('commission_form', 'config_draw') }}

),

renamed as (

    select
        employee_name,
        employee_email,
        bonus_plan,
        draw_start_date,
        draw_end_date,
        draw_amount,
        draw_max,
        balance_owed,
        note,
        effective_from

    from source

)

select * from renamed

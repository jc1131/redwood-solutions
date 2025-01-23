with 

source as (

    select * from {{ source('commission_form', 'config_commission_relationship') }}

),

renamed as (

    select
        primary_employee__name,
        primary_employee_email,
        secondary_employee_name,
        secondary__employee_email,
        commission_rate,
        commission_hold_days,
        effective_from

    from source

)

select * from renamed

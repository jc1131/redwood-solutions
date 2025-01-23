with 

source as (

    select * from {{ source('commission_form', 'config_commission') }}

),

renamed as (

    select
        employee_name,
        employee_email,
        commission_tier,
        lower_amount,
        higher_amount,
        commission_percentage,
        effective_from

    from source

)

select * from renamed

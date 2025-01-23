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
        effective_from,
        'config_commission' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source

),

pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as config_commission_pk
    ,*
    from renamed
)

select * from pk_generation

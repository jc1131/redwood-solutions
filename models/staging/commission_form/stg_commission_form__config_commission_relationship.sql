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
        effective_from,
        'config_commission_relationship' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source

),

pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as config_commission_relationship_pk
    ,*
    from renamed
)

select * from pk_generation
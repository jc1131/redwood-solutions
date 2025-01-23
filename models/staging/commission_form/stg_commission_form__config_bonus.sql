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
        effective_from,
        'config_bonus' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source

),

pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as config_bonus_pk
    ,*
    from renamed
)

select * from pk_generation

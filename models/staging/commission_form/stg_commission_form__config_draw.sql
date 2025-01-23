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
        effective_from,
        'config_draw' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source

),

pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as config_draw_pk
    ,*
    from renamed
)

select * from pk_generation
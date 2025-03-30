with 

source as (

    select * from {{ source('commission_form', 'form_payment') }}

),

renamed as (

    select
        string_field_0 as job_order_number,
        string_field_1 as payment_received_date,
        string_field_2 as recruiter_name,
       'form_payment' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source

), pk_generation as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as form_payment_pk
        ,*
    from renamed
)

select * from pk_generation

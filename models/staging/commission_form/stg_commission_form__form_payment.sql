with 

source as (

    select * from {{ source('commission_form', 'form_payment') }}

),

renamed as (

    select
        Job_Order_Number as job_order_number,
        DATE(Payment_Received_Date) as payment_received_date,
        Employee__Name as recruiter_name,
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

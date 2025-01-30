with 

source as (

    select * from {{ source('commission_form', 'form_activity') }}

),

renamed as (

    select
        timestamp as last_modified,
        email_address as last_modified_by,
        activity_bonus_month,
        trim(name) as activity_bonus_recipient,
        'form_activity' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source,
    UNNEST(SPLIT(activity_bonus_recipient_s__, ',')) AS name

),

pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as form_activity_pk
    ,*
    from renamed
)

select * from pk_generation
with base_commission as (
    select * from {{ ref('int_commission') }}
),
config_draw as (
    select * from {{ ref('stg_commission_form__config_draw') }}
)

select * from base_commission
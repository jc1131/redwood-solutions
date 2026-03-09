/*
  int_payout
  ──────────
  Grain  : one row per payout event (commission or bonus) per recruiter.
  Sources: int_commission, int_bonus
  Single union point in the DAG.
*/

with commission_data as (

    select * from {{ ref('int_commission') }}

),

bonus as (

    select * from {{ ref('int_bonus') }}

),

commission_rows as (

    select
        commission_data.commission_pk                               as payout_pk,
        'commission'                                                as payout_type,
        cast(null as string)                                        as bonus_type,
        commission_data.form_response_pk                            as form_response_fk,
        commission_data.recruiter_name,
        commission_data.commission_pay_date,
        cast(commission_data.invoice_amount as numeric)             as invoice_amount,
        cast(commission_data.total_invoice_ytd as numeric)          as total_invoice_ytd,
        cast(commission_data.total_commission_sales as numeric)     as total_commission_sales,
        cast(commission_data.invoice_credit_percent as numeric)     as invoice_credit_percent,
        cast(commission_data.current_tier_rate as numeric)          as current_tier_rate,
        cast(commission_data.commission as numeric)                 as commission_amount,
        cast(null as numeric)                                       as bonus_amount,
        concat('Commission - ', commission_data.form_detail_description) as payout_description,
        cast(commission_data.job_order_number as string)            as job_order_number,
        commission_data.client_name,
        commission_data.candidate_name,
        commission_data.due_date,
        commission_data.last_modified,
        commission_data.payment_received_date,
        commission_data.is_valid_split

    from commission_data

),

bonus_rows as (

    select
        bonus_pk                        as payout_pk,
        'bonus'                         as payout_type,
        bonus_type,
        cast(null as string)            as form_response_fk,
        recruiter_name,
        bonus_pay_date                  as commission_pay_date,
        cast(null as numeric)           as invoice_amount,
        cast(null as numeric)           as total_invoice_ytd,
        cast(null as numeric)           as total_commission_sales,
        cast(null as numeric)           as invoice_credit_percent,
        cast(null as numeric)           as current_tier_rate,
        cast(null as numeric)           as commission_amount,
        cast(bonus_amount as numeric)   as bonus_amount,
        bonus_description               as payout_description,
        cast(null as string)            as job_order_number,
        cast(null as string)            as client_name,
        cast(null as string)            as candidate_name,
        cast(null as date)              as due_date,
        cast(null as timestamp)         as last_modified,
        cast(null as date)              as payment_received_date,
        cast(null as bool)              as is_valid_split

    from bonus

),

final as (

    select * from commission_rows
    union all
    select * from bonus_rows

)

select * from final
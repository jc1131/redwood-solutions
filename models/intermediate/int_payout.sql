/*
  int_payout
  ──────────
  Grain  : one row per payout event (commission or bonus) per recruiter.
  Sources: int_commission
           int_bonus

  This is the single union point in the entire DAG. Its only job is to
  combine the two streams and pad NULLs so both shapes share one column
  schema. All business logic belongs upstream.

  The fact_commission_payout mart reads exclusively from this model,
  meaning any future schema changes to the output only require edits here
  and in the mart — not across multiple intermediates.

  Column schema
  ─────────────
  payout_pk             unique identifier for every payout row
  payout_type           'commission' | 'bonus'
  bonus_type            null for commissions, else bonus subtype
  form_response_fk      FK back to form_response (null for bonuses)
  recruiter_name
  commission_pay_date   when the payout is due
  invoice_amount        null for bonuses
  total_invoice_ytd     null for bonuses
  total_commission_sales null for bonuses
  invoice_credit_percent null for bonuses
  commission_amount     null for bonuses
  bonus_amount          null for commissions
  payout_description    human-readable explanation
  -- Pass-through context columns for the mart (null for bonus rows)
  job_order_number
  client_name
  candidate_name
  due_date
  last_modified
  payment_received_date
  is_valid_split
*/

with commission as (

    select * from {{ ref('int_commission') }}

),

bonus as (

    select * from {{ ref('int_bonus') }}

),

commission_rows as (

    select
        commission_pk                   as payout_pk,
        'commission'                    as payout_type,
        cast(null as string)            as bonus_type,

        -- FK for mart join to invoice context
        form_response_pk                as form_response_fk,

        recruiter_name,
        commission_pay_date,

        -- Invoice amounts
        invoice_amount,
        total_invoice_ytd,
        total_commission_sales,
        invoice_credit_percent,
        commission                      as commission_amount,
        cast(null as numeric)           as bonus_amount,

        -- Description
        concat(
            'Commission — ', form_detail_description
        )                               as payout_description,

        -- Context pass-through (used by mart, avoids re-joining)
        job_order_number,
        client_name,
        candidate_name,
        due_date,
        last_modified,
        payment_received_date,
        is_valid_split

    from commission

),

bonus_rows as (

    select
        bonus_pk                        as payout_pk,
        'bonus'                         as payout_type,
        bonus_type,

        cast(null as string)            as form_response_fk,

        recruiter_name,
        bonus_pay_date                  as commission_pay_date,

        -- Invoice amounts — null for bonus rows
        cast(null as numeric)           as invoice_amount,
        cast(null as numeric)           as total_invoice_ytd,
        cast(null as numeric)           as total_commission_sales,
        cast(null as numeric)           as invoice_credit_percent,
        cast(null as numeric)           as commission_amount,
        cast(bonus_amount as numeric)   as bonus_amount,

        bonus_description               as payout_description,

        -- Context — null for bonus rows
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
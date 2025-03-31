with payout_combined as (
    select * from {{ ref('int_payout_combined') }}
),dim_date as (
    select * from {{ ref('dim_date') }}
),
form_header as (
    select * from {{ ref('int_response_header')}}
)
select
    payout_combined.commission_pk			
    ,payout_combined.form_response_fk		
    ,form_header.last_modified
    ,form_header.job_order_number
    ,form_header.client_name
    ,form_header.candidate_name	
    ,payout_combined.recruiter_name			
    ,payout_combined.invoice_payment_date			
    ,payout_combined.commission_pay_date	
    ,dim_date.next_pay_date
    ,payout_combined.invoice_amount			
    ,payout_combined.total_invoice_ytd			
    ,payout_combined.commission_percentage			
    ,payout_combined.commission_amount			
    ,payout_combined.other_comm_and_bonus			
    ,payout_combined.payout_description			

from payout_combined
    left join dim_date on dim_date.date_day = payout_combined.commission_pay_date
    left join form_header on form_header.form_response_pk = payout_combined.form_response_fk
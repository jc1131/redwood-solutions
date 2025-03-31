with payout_combined as (
    select * from {{ ref('int_payout_combined') }}
),dim_date as (
    select * from {{ ref('dim_date') }}
)
select
    payout_combined.commission_pk			
    ,payout_combined.form_response_fk			
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
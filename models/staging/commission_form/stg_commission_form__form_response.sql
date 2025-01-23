with 

source as (

    select * from {{ source('commission_form', 'form_response') }}

),

renamed as (

    select
        timestamp,
        email_address,
        job_order_number,
        client_name,
        new_client_needing_fieldpros_w9_,
        client_billing_address_,
        should_the_invoice_description_include_the_fee_calculation__including_the_candidate_s_salary_,
        remit_client_billing,
        fieldpros_account_manager,
        candidate_name,
        position_title_,
        client_hiring_manager,
        billing_percentage_rate,
        candidate_s_base_salary,
        invoice_amount_,
        work_start_date,
        offer_signature_date,
        invoice_payment_terms,
        invoice_payment_date,
        agreement_job_order_percentage,
        agreement_job_order_recruiter,
        account_manager_percentage,
        account_manager_recruiter,
        working_candidate_percentage_,
        working_candidate_recruiter,
        candidate_ownership_percentage_,
        candidate_ownership_recruiter

    from source

)

select * from renamed

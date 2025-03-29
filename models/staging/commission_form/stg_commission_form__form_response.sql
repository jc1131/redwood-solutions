with 

source as (

    select * from {{ source('commission_form', 'form_response') }}

),

renamed as (

    select
        timestamp as last_modified,
        email_address,
        job_order_number,
        client_name,
        new_client_needing_fieldpros_w9_ as is_new_w9,
        Client_Billing_Address as client_billing_address,
        should_the_invoice_description_include_the_fee_calculation__including_the_candidate_s_salary_ as is_fee_calculation,
        Remit_Client_Billing_ as remit_client_billing,
        candidate_name,
        Position_Title as position_title,
        client_hiring_manager,
        billing_percentage_rate,
        candidate_s_base_salary as candidate_base_salary,
        Invoice_Amount as invoice_amount,
        work_start_date,
        offer_signature_date,
        invoice_payment_terms,
        cast(REGEXP_EXTRACT(invoice_payment_terms, r'\b\d+\b') as int) AS payment_term_number,
        case 
        when Reference_Date_for_Terms = 'Work Start Date' then work_start_date
        when Reference_Date_for_Terms = 'Offer Signature Date' then offer_signature_date
        else null
        end payment_term_date,
        Reference_Date_for_Terms as invoice_payment_date,
        agreement_job_order_percentage,
        agreement_job_order_recruiter,
        account_manager_percentage,
        account_manager_recruiter,
        working_candidate_percentage_ as working_candidate_percentage,
        working_candidate_recruiter,
        candidate_ownership_percentage_ as candidate_ownership_percentage,
        candidate_ownership_recruiter,
      'form_response' as source_key,
        ROW_NUMBER() OVER() source_row_number

    from source

),

pk_generation as (
    select
    {{ dbt_utils.generate_surrogate_key(['source_key', 'source_row_number']) }} as form_response_pk
    ,*
    from renamed
)

select * from pk_generation

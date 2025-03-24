WITH commission_calc_base_line AS (
    SELECT * FROM {{ ref('int_response_detail') }}
),
commission_calc_base_header AS (
    SELECT * FROM {{ ref('int_response_header') }}
),commission_config AS (
    SELECT * FROM {{ ref('stg_commission_form__config_commission') }}
),
deal_side_agg as (
  select
    form_response_fk
    ,recruiter_name
    ,recruiter_email
    ,sum(recruiter_credit_percentage) as invoice_credit_percent
    ,STRING_AGG( distinct form_detail_description) as form_detail_description
  from commission_calc_base_line
  group by all
),
commission_calc_base as (
  select
 header.invoice_amount
  ,header.job_order_number 
  ,header.due_date
  ,agg.*
  ,agg.invoice_credit_percent * header.invoice_amount as invoive_credit_amount
  ,SUM(agg.invoice_credit_percent * header.invoice_amount) OVER (PARTITION BY recruiter_email order by header.due_date asc) as total_commission_sales

  from deal_side_agg agg
    left join commission_calc_base_header header
      on agg.form_response_fk = header.form_response_pk
),
tier_calc_base as (
select 
commission_calc_base.*,
  commission_config.commission_tier,
  commission_config.lower_amount,
  commission_config.higher_amount,
  commission_config.commission_percentage,
  -- Calculate the tier amount
  LEAST(commission_calc_base.total_commission_sales, commission_config.higher_amount) 
    - GREATEST(commission_calc_base.total_commission_sales - commission_calc_base.invoive_credit_amount, commission_config.lower_amount) AS tier_amount,
  -- Calculate the commission for this tier
  (LEAST(commission_calc_base.total_commission_sales, commission_config.higher_amount) 
    - GREATEST(commission_calc_base.total_commission_sales - commission_calc_base.invoive_credit_amount, commission_config.lower_amount)) 
    * commission_config.commission_percentage AS tier_commission
FROM commission_calc_base
    JOIN commission_config 
      ON commission_calc_base.total_commission_sales >= commission_config.lower_amount 
      AND commission_calc_base.total_commission_sales - commission_calc_base.invoive_credit_amount <= commission_config.higher_amount
      AND commission_calc_base.recruiter_email = commission_config.employee_email
),
final as (
  select
commission_calc_base.*
,sum(cast(tier_calc_base.tier_commission as numeric)) as commission
,row_number() OVER (PARTITION BY commission_calc_base.recruiter_email order by commission_calc_base.due_date asc) as commission_number

  from commission_calc_base
  left join tier_calc_base on commission_calc_base.form_response_fk = tier_calc_base.form_response_fk
  and commission_calc_base.recruiter_email = tier_calc_base.recruiter_email
  group by all
),pk_generation as (
    select 
    {{ dbt_utils.generate_surrogate_key(['form_response_fk', 'commission_number']) }} as commission_pk,
    *
 from final
)
select * from pk_generation
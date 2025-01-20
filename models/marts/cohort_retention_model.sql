WITH
    invoice AS (SELECT * FROM {{ ref("stg_invoice") }}),
    cohort_diff AS (
    SELECT
    invoice.*
    ,invoice_year - year_aquired as year_difference
    ,invoice_month - month_aquired as month_difference
    from invoice
    where year_aquired >= 2022.0
    ),
    final as(
        select 
        *
        ,(year_difference * 12) + month_difference + 1 as cohort_index
        from cohort_diff
    )
select * from final
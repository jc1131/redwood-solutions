select
string_field_0 as invoice_number
,string_field_1 as invoice_customer
,string_field_2 as invoice_date
,cast(left(string_field_2,2) as numeric) as invoice_month
,cast(right(string_field_2,4) as numeric) as invoice_year
,string_field_3 as customer_acquired
,cast(left(string_field_3,4)as numeric) as year_aquired
,cast(CASE 
        WHEN trim(substring(string_field_3,5)) = 'January' THEN 1
        WHEN trim(substring(string_field_3,5)) = 'February' THEN 2
        WHEN trim(substring(string_field_3,5)) = 'March' THEN 3
        WHEN trim(substring(string_field_3,5)) = 'April' THEN 4
        WHEN trim(substring(string_field_3,5)) = 'May' THEN 5
        WHEN trim(substring(string_field_3,5)) = 'June' THEN 6
        WHEN trim(substring(string_field_3,5)) = 'July' THEN 7
        WHEN trim(substring(string_field_3,5)) = 'August' THEN 8
        WHEN trim(substring(string_field_3,5)) = 'September' THEN 9
        WHEN trim(substring(string_field_3,5)) = 'October' THEN 10
        WHEN trim(substring(string_field_3,5)) = 'November' THEN 11
        WHEN trim(substring(string_field_3,5)) = 'December' THEN 12
        ELSE NULL -- Handle other cases if necessary
    END as numeric) AS month_aquired
,string_field_4 as invoice_item
,string_field_5 as invoice_quantity
,string_field_6  as invoice_amount
from exalted-slate-410901.dbt_jcrist.raw_invoice
where string_field_3 is not null
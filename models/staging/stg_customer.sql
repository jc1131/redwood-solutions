with customer as (
    select * from `exalted-slate-410901.dynamic_designs.invoice`
),customer_start as (
    select
        customerID as customer_id
        ,cast(min(InvoiceDate)as date) as relationship_start
    from customer
    group by 1
),final as (
    select 
    customer_id
    ,relationship_start
    ,extract(year FROM relationship_start) as relationship_year
    ,DATE_DIFF(current_date(), relationship_start, year) AS num_year

    from customer_start
)
select * from final
order by relationship_year desc
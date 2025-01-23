WITH base_sales AS (
    SELECT
        salesperson AS primary_salesperson,
        deal_amount
    FROM {{ ref('sales') }}  -- Replace 'sales' with your actual sales table
),

secondary_commissions AS (
    SELECT
        cr.secondary_salesperson AS salesperson,
        bs.deal_amount * cr.commission_rate AS deal_amount,
        CONCAT('Commission from ', bs.primary_salesperson) AS deal_description
    FROM base_sales bs
    INNER JOIN {{ ref('commission_relationships') }} cr
        ON bs.primary_salesperson = cr.primary_salesperson
)

SELECT 
    primary_salesperson AS salesperson,
    deal_amount,
    'Direct Deal' AS deal_description
FROM base_sales

UNION ALL

SELECT
    salesperson,
    deal_amount,
    deal_description
FROM secondary_commissions

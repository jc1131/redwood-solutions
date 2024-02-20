WITH
rfm_summary AS (SELECT * FROM {{ ref("stg_rfm") }}),

rfm_category AS (SELECT * FROM {{ ref("stg_category") }}),

stg_customer AS (SELECT * FROM {{ ref("stg_customer") }}),

final AS (
    SELECT
        rfm_s.customerid AS customer_name,
        rfm_s.recency AS days_since_last_order,
        rfm_s.frequency AS number_of_orders,
        cast(rfm_s.monetary AS numeric) AS total_sales,
        CASE
            WHEN rfm_s.monetary < 1000 THEN 'Do Not Call' ELSE
                rfm_c.rfm_category
        END AS customer_segment,
        CASE
            WHEN rfm_s.monetary < 1000
                THEN 'Do Not Call'
            WHEN cust.num_year > 3 AND rfm_s.monetary < 1000
                THEN 'Loyal'
            WHEN rfm_s.monetary > 1000 AND rfm_s.monetary < 7500
                THEN 'Loyal'
            WHEN rfm_s.monetary > 7500
                THEN 'Champion'
            ELSE rfm_c.rfm_category
        END AS customer_segment_adjusted,
        rfm_s.r_quartile + 1 AS r_quart,
        rfm_s.f_quartile + 1 AS f_quart
    FROM rfm_summary AS rfm_s
    LEFT JOIN rfm_category AS rfm_c ON rfm_s.rfm_score = rfm_c.rfm_score
    LEFT JOIN stg_customer AS cust ON rfm_s.customerid = cust.customer_id
)

SELECT *
FROM final

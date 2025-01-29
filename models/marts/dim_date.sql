{{
    config(
        materialized = "table"
    )
}}

WITH date_dimension AS (
    {{ dbt_date.get_date_dimension('2024-01-01', '2026-12-31') }}
),
fiscal_periods AS (
    {{ dbt_date.get_fiscal_periods('date_dimension', year_end_month=1, week_start_day=1, shift_year=1) }}
)
SELECT
    d.*,
    f.fiscal_week_of_year,
    f.fiscal_week_of_period,
    f.fiscal_period_number,
    f.fiscal_quarter_number,
    f.fiscal_period_of_quarter,
    
    -- Determine pay date based on semi-monthly cycle
    CASE 
        WHEN EXTRACT(DAY FROM d.date_day) <= 15 THEN DATE_TRUNC(d.date_day, MONTH) + INTERVAL 14 DAY
        ELSE LAST_DAY(d.date_day)
    END AS pay_date,
    
    -- Determine pay cycle start
    CASE 
        WHEN EXTRACT(DAY FROM d.date_day) <= 15 THEN DATE_TRUNC(d.date_day, MONTH)
        ELSE DATE_TRUNC(d.date_day, MONTH) + INTERVAL 15 DAY
    END AS pay_cycle_start,

    -- Determine pay cycle end
    CASE 
        WHEN EXTRACT(DAY FROM d.date_day) <= 15 THEN DATE_TRUNC(d.date_day, MONTH) + INTERVAL 14 DAY
        ELSE LAST_DAY(d.date_day)
    END AS pay_cycle_end

FROM date_dimension d
LEFT JOIN fiscal_periods f
    ON d.date_day = f.date_day
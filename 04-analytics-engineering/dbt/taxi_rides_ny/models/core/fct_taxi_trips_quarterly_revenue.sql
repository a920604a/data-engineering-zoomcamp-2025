{{
    config(
        materialized='table'
    )
}}

with temp as (
    SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, 
EXTRACT(QUARTER FROM pickup_datetime) AS quarter, service_type, total_amount
    from {{ ref('fact_trips') }}
),
grouped as (
    select service_type, year, quarter, sum(total_amount) as total_amount from temp
group by service_type, year, quarter
)
SELECT 
    service_type,
    year,
    quarter,
    total_amount,
    LAG(total_amount) OVER (
        PARTITION BY service_type, quarter ORDER BY year
    ) AS prev_year_total_amount,
    CASE 
        WHEN LAG(total_amount) OVER (
            PARTITION BY service_type, quarter ORDER BY year
        ) = 0 THEN NULL  -- Avoid division by zero
        ELSE ROUND(
            (total_amount - LAG(total_amount) OVER (
                PARTITION BY service_type, quarter ORDER BY year
            )) / NULLIF(LAG(total_amount) OVER (
                PARTITION BY service_type, quarter ORDER BY year
            ), 0) * 100, 2
        )
    END AS yoy_percentage_change
FROM grouped
ORDER BY service_type, year, quarter
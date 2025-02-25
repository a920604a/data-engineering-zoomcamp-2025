WITH quarterly_revenue AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        SUM(total_amount) AS revenue
        from {{ ref("fact_trips") }}
    WHERE total_amount > 0  -- 避免異常數據
    GROUP BY service_type, year, quarter
),

yoy_growth AS (
    SELECT
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.revenue,
        q2.revenue AS prev_year_revenue,
        ROUND(
            SAFE_DIVIDE(q1.revenue - q2.revenue, q2.revenue) * 100, 
            2
        ) AS yoy_growth  -- 使用 SAFE_DIVIDE 避免除以零錯誤
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.service_type = q2.service_type
        AND q1.year = q2.year + 1  -- 連接去年同期數據
        AND q1.quarter = q2.quarter
)

SELECT * FROM yoy_growth
ORDER BY service_type, year, quarter

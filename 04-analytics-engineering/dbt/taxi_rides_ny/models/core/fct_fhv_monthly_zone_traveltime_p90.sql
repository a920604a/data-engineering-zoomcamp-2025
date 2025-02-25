WITH fhv_trips AS (
    SELECT
        pickup_location_id,
        dropoff_location_id,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        trip_duration  -- 假設 trip_duration 為旅行時間 (秒/分鐘)
    FROM {{ ref('fact_trips') }}
    WHERE service_type = 'FHV'  -- 只選取 FHV 類型
),

p90_travel_time AS (
    SELECT
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY trip_duration) AS p90_duration
    FROM fhv_trips
    GROUP BY pickup_location_id, dropoff_location_id, year, month
),

ranked_travel_time AS (
    SELECT
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        p90_duration,
        RANK() OVER (ORDER BY p90_duration DESC) AS rank_nth  -- 根據 P90 時間排序
    FROM p90_travel_time
)

SELECT *
FROM ranked_travel_time
WHERE rank_nth = {{ nth_value }};  -- 替換 `nth_value` 為你要查詢的排名

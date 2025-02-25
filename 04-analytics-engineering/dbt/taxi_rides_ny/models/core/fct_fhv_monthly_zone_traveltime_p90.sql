WITH trip_dur_perc AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        year,
        month,
        --TIMESTAMP_DIFF(pickup_datetime, dropoff_datetime, SECOND) AS trip_dur_secs,
        PERCENTILE_CONT(TIMESTAMP_DIFF(pickup_datetime, dropoff_datetime, SECOND), 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS p90
    FROM {{ ref('dim_fhv_trips') }}
)

-- Compute the **continous** `p90` of `trip_duration` partitioning by
-- year, month, pickup_location_id, and dropoff_location_id

SELECT * FROM trip_dur_perc
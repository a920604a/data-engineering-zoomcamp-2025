1. `select * from myproject.raw_nyc_tripdata.ext_green_taxi`
2. Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
3. dbt run --select models/staging/+
4. 
Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile
When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
5. 

- [here](./dbt/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql)

```sql
SELECT * FROM keen-dolphin-450409-m8.dbt_ychen.fct_taxi_trips_quarterly_revenue

WHERE year = 2020

ORDER BY total_amount DESC; 

```
6. 


`dbt build --vars "{'is_test_run': false}"`

- [here](./dbt/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql)

```sql
SELECT service_type, year, month, p97, p95, p90
FROM `keen-dolphin-450409-m8.dbt_ychen.fct_taxi_trips_monthly_fare_p95`
WHERE year = 2020 and month = 4
GROUP BY service_type, year, month, p97, p95, p90
```
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}


7. 

LaGuardia Airport, Chinatown, Garment District
1. `select * from myproject.raw_nyc_tripdata.ext_green_taxi`
2. Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
3. 
4. 
5. 
6. 
7. 
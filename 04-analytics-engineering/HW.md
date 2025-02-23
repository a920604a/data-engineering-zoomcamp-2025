
```sql
CREATE TABLE `keen-dolphin-450409-m8.trips_data_all.green_tripdata` AS
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`;

CREATE TABLE `keen-dolphin-450409-m8.trips_data_all.yellow_tripdata` AS
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`;


INSERT INTO `keen-dolphin-450409-m8.trips_data_all.yellow_tripdata` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;


INSERT INTO `keen-dolphin-450409-m8.trips_data_all.green_tripdata` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;


select count(*) from `keen-dolphin-450409-m8.trips_data_all.green_tripdata` ;
select count(*) from `keen-dolphin-450409-m8.trips_data_all.yellow_tripdata` ;
```
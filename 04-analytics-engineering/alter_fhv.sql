ALTER TABLE `keen-dolphin-450409-m8.trips_data_all.fhv_tripdata`
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `keen-dolphin-450409-m8.trips_data_all.fhv_tripdata`
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
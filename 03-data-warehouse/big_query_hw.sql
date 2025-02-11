CREATE OR REPLACE EXTERNAL TABLE `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-data-lake-tim/yellow_tripdata_2024-*.parquet']
);



SELECT count(*) FROM `keen-dolphin-450409-m8.nytaxi.fhv_tripdata`;


SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `keen-dolphin-450409-m8.nytaxi.fhv_tripdata`;


CREATE OR REPLACE TABLE `keen-dolphin-450409-m8.nytaxi.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `keen-dolphin-450409-m8.nytaxi.fhv_tripdata`;

CREATE OR REPLACE TABLE `keen-dolphin-450409-m8.nytaxi.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `keen-dolphin-450409-m8.nytaxi.fhv_tripdata`
);

SELECT count(*) FROM  `keen-dolphin-450409-m8.nytaxi.fhv_nonpartitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `keen-dolphin-450409-m8.nytaxi.fhv_partitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

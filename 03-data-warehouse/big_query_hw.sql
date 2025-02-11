CREATE OR REPLACE EXTERNAL TABLE `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-data-lake-tim/yellow_tripdata_2024-*.parquet']
);

CREATE TABLE `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`
AS
SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`;


CREATE MATERIALIZED VIEW `keen-dolphin-450409-m8.nytaxi.materialized_yellow_taxi_data`
AS
SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`;


SELECT count(*) FROM `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`;


SELECT COUNT(DISTINCT PULocationID) FROM keen-dolphin-450409-m8.nytaxi.external_yellow_taxi;

SELECT COUNT(DISTINCT PULocationID) FROM keen-dolphin-450409-m8.nytaxi.materialized_yellow_taxi_data;

SELECT COUNT(*) FROM keen-dolphin-450409-m8.nytaxi.yellow_taxi_data WHERE fare_amount = 0;



CREATE OR REPLACE TABLE `keen-dolphin-450409-m8.nytaxi.partition_yellow_taxi_data`
PARTITION BY DATE(tpep_dropoff_datetime )  -- 按日期分區 
AS SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`;

SELECT DISTINCT VendorID
FROM `keen-dolphin-450409-m8.nytaxi.partition_yellow_taxi_data`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


SELECT COUNT(*) 
FROM `keen-dolphin-450409-m8.nytaxi.materialized_yellow_taxi_data`;

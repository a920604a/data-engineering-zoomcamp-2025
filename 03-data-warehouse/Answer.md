Creating an External Table in BigQuery using Yellow Taxi Trip Records
```sql
CREATE EXTERNAL TABLE `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`
OPTIONS (
  format = 'CSV',  -- Replace with 'PARQUET' or 'JSON' if your data is in those formats
  uris = ['gs://dtc-data-lake-tim/yellow_taxi_trip_records/*.csv']
)
AS
SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`;
``` 

Creating a Regular Table in BigQuery using Yellow Taxi Trip Records
```sql
CREATE TABLE `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`
AS
SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`;
```

Creating a Materialized Table in BigQuery using Yellow Taxi Trip Records
```sql
CREATE MATERIALIZED VIEW `keen-dolphin-450409-m8.nytaxi.materialized_yellow_taxi_data`
AS
SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`;
```

## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 20,332,093
> SELECT COUNT(*) FROM `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`;


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 155.12 MB for the Materialized Table

> SELECT COUNT(DISTINCT PULocationID) 
FROM `keen-dolphin-450409-m8.nytaxi.external_yellow_taxi`;

> SELECT COUNT(DISTINCT PULocationID) 
FROM `keen-dolphin-450409-m8.nytaxi.materialized_yellow_taxi_data`;

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4:
How many records have a fare_amount of 0?
- 8,333

> SELECT COUNT(*) 
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`
WHERE fare_amount = 0;

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
- Partition by tpep_dropoff_datetime and Cluster on VendorID


## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```sql
CREATE OR REPLACE TABLE `keen-dolphin-450409-m8.nytaxi.partition_yellow_taxi_data`
PARTITION BY DATE(tpep_dropoff_datetime )  -- 按日期分區 
AS SELECT *
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`;

SELECT DISTINCT VendorID
FROM `keen-dolphin-450409-m8.nytaxi.partition_yellow_taxi_data`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```
```sql
SELECT DISTINCT VendorID
FROM `keen-dolphin-450409-m8.nytaxi.yellow_taxi_data`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

## Question 7: 
Where is the data stored in the External Table you created?
- GCP Bucket

## Question 8:
It is best practice in Big Query to always cluster your data:
- False
> 


## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
- 0
> SELECT COUNT(*) 
FROM `keen-dolphin-450409-m8.nytaxi.materialized_yellow_taxi_data`;

> Since a materialized view (Materialized View) is a table that calculates and stores the results in advance, BigQuery will use the stored results for query instead of recalculating. This makes queries execute faster and reduces the number of bytes scanned
## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw3

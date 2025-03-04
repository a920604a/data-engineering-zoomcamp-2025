from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

spark = SparkSession.builder \
    .appName("YellowTaxiOctober2024") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 讀取 Parquet 檔案
df_yellow = spark.read.parquet("yellow_tripdata_2024-10.parquet")



# 計算行程時長 (小時)
df_yellow = df_yellow.withColumn(
    "trip_duration_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)

# 取得最長的行程時間
longest_trip = df_yellow.agg({"trip_duration_hours": "max"}).collect()[0][0]
print(f"Longest trip duration: {longest_trip:.2f} hours")
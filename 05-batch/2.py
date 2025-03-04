from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YellowTaxiOctober2024") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 讀取 Parquet 文件
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# 檢查 Schema
df.printSchema()


df = df.repartition(4)  # 設定 4 個 partition
df.write.mode("overwrite").parquet("yellow_oct_2024_partitioned.parquet")


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("YellowTaxiOctober2024") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 讀取 Parquet 檔案
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# 檢查 Schema，確認有 `tpep_pickup_datetime` 欄位
df.printSchema()



from pyspark.sql.functions import to_date

# 轉換日期格式，過濾 10 月 15 日的行程
df_filtered = df.filter(to_date(col("tpep_pickup_datetime")) == "2024-10-15")

# 計算行程數
trip_count = df_filtered.count()
print(f"Total trips on October 15th: {trip_count}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, asc

# 創建 Spark Session
spark = SparkSession.builder \
    .appName("YellowTaxiAnalysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 讀取 Parquet 檔案（計程車數據）
df_yellow = spark.read.parquet("yellow_tripdata_2024-10.parquet")

# 讀取 CSV（計程車地區對照表）
df_zone = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)

# 檢查 Schema
df_zone.printSchema()
df_yellow.printSchema()


# 設定 Temp Table
df_yellow.createOrReplaceTempView("yellow")
df_zone.createOrReplaceTempView("zone")

# 計算最少的上車地點
least_frequent_zone = spark.sql("""
    SELECT z.Zone, COUNT(y.PULocationID) AS pickup_count
    FROM yellow y
    JOIN zone z ON y.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY pickup_count ASC
    LIMIT 1
""")

least_frequent_zone.show()

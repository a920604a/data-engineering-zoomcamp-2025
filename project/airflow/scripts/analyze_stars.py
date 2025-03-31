from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# 初始化 Spark
spark = SparkSession.builder \
    .appName("GHArchive Star Analysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 讀取 PostgreSQL 數據
jdbc_url = "jdbc:postgresql://postgres-dz:5432/zoomcamp"
properties = {"user": "zoomcamp", "password": "zoomcamp", "driver": "org.postgresql.Driver"}

df = spark.read.jdbc(url=jdbc_url, table="watch_events", properties=properties)

# 統計 Star 成長最快的 Repo
df_result = df.groupBy("repo_name").agg(count("*").alias("star_growth")).orderBy("star_growth", ascending=False)

# 儲存結果回 PostgreSQL
df_result.write.jdbc(url=jdbc_url, table="star_growth", mode="overwrite", properties=properties)

spark.stop()

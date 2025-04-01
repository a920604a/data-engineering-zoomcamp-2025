from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
import gzip
import json
from io import StringIO
import os
import itertools
import logging
from pathlib import Path
import pandas as pd
import psycopg2
from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession

# 設定 Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 設定 Google Cloud Storage 和 BigQuery 相關參數
GCS_BUCKET = "dz-data-lake" 
GCS_PATH = "gharchive"
BQ_PROJECT = "dz-final-project"
BQ_DATASET = "gharchive"
BQ_DATASET_AREA = "US"
BQ_TABLE = "watch_events"


# # PostgreSQL 連線配置
# DB_CONN = {
#     "dbname": "zoomcamp",
#     "user": "zoomcamp",
#     "password": "zoomcamp",
#     "host": "postgres-dz",
#     "port": "5432",
# }
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")



with DAG(
    dag_id="cloud_gharchive_dag",
    default_args=default_args,
    # schedule_interval="0 * * * *",  # 每小時執行
    schedule_interval=None,  # 移除排程，僅手動觸發
    catchup=False,
) as dag:

    now = datetime.utcnow() - timedelta(hours=2)  # 使用 UTC 時間
    date_str = now.strftime('%Y-%m-%d')
    current_hour = now.hour
    dataset_file = f"{date_str}-{current_hour}.json.gz"
    dataset_url = f"https://data.gharchive.org/{dataset_file}"
    local_gz_path = f"{path_to_local_home}/data/{dataset_file}"
    local_json_path = local_gz_path.replace(".gz", "")
    gcs_gz_path = f"{GCS_PATH}/{dataset_file}"

    os.makedirs(f"{path_to_local_home}/data", exist_ok=True)

    # 下載 GH Archive 數據
    fetch_data_task = BashOperator(
        task_id="fetch_data",
        bash_command=f"wget {dataset_url} -O {local_gz_path} && gzip -d {local_gz_path}"
    )
    
    def ingest_and_save_data(dataset_name: str):
        print(f"{dataset_name}")
        '''直接讀取 JSON 並儲存為 Parquet'''
        path_to_json = f"{path_to_local_home}/data/{dataset_name}"
        path_to_parquet = f"{path_to_local_home}/data/{dataset_name}.parquet"
        
        try:
            dfs = []
            with open(path_to_json, 'r') as f:
                while True:
                    lines = list(itertools.islice(f, 1000))
                    if not lines:
                        break
                    dfs.append(pd.read_json(StringIO(''.join(lines)), lines=True))

            df = pd.concat(dfs)
            
            
            
            df["created_at"] = pd.to_datetime(df["created_at"])

            # 移除時區（轉為 naive datetime）
            df["created_at"] = df["created_at"].dt.tz_localize(None)

            # 轉換為 datetime64[ms]（毫秒精度）
            df["created_at"] = df["created_at"].astype("datetime64[ms]")

            # 重新儲存成 Parquet
            df.to_parquet(f"{path_to_parquet}", engine="pyarrow", compression="gzip")
        
        

            # 儲存為 Parquet
            # df.to_parquet(path_to_parquet, compression="gzip")
            logger.info(f"成功儲存 Parquet: {path_to_parquet}")

            # 刪除 JSON 檔案以節省空間
            os.remove(path_to_json)
            logger.info(f"已刪除 JSON 檔案: {path_to_json}")

        except Exception as e:
            logger.error(f"處理資料時發生錯誤: {e}")
            raise

        return path_to_parquet  # 但不會存入 XCom

    ingest_and_save_task = PythonOperator(
        task_id="ingest_and_save",
        python_callable=ingest_and_save_data,
        op_kwargs={"dataset_name": dataset_file.replace(".gz", "")},
        do_xcom_push=False  # 避免將大資料存入 XCom
    )
    
    # def clean_with_spark(local: str):
    #     print(f"local: {local}")
        
    #     # 檢查檔案是否存在
    #     if not os.path.exists(local):
    #         raise FileNotFoundError(f"The file at {local} does not exist.")
        
    #     # 啟動 SparkSession
    #     spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()
            

        
    #     # 讀取 BigQuery 資料
    #     df = spark.read.option("header", "true").parquet(f'{local}')

        
    #     # 過濾 PushEvent 並計算 push 次數
    #     df_filtered = df.filter(col("type") == "PushEvent") \
    #             .groupBy("repo.name") \
    #             .agg(count("*").alias("push_count")) \
    #             .orderBy(col("push_count").desc())

    #     # 確保輸出路徑為資料夾，而不是檔案
    #     output_dir = os.path.join(local, "data/")
        
    #     # 如果資料夾不存在，創建資料夾
    #     if not os.path.exists(output_dir):
    #         os.makedirs(output_dir)

    #     # 寫入 Parquet 資料
    #     df_filtered.write.parquet(output_dir, mode='overwrite')



    #     print(f"資料處理完成，儲存至 {output_dir}")
       

    # spark_clean_task = PythonOperator(
    #     task_id=f'spark_clean',
    #     python_callable=clean_with_spark,
    #     op_kwargs={            
    #         "local": f"{path_to_local_home}/data/{dataset_file.replace('.parquet', '')}"
    #     }
    # )
        
        
    
    # 將 Parquet 檔案上傳至 GCS
    # load_gcs_task = LocalFilesystemToGCSOperator(
    #     task_id="load_gcs",
    #     src=f"{path_to_local_home}/data/{dataset_file.replace('.gz', '')}.parquet",
    #     dst=gcs_gz_path.replace(".gz", ".parquet"),
    #     bucket=GCS_BUCKET,
    #     mime_type="application/octet-stream",
    # )

    # load_bigquery_task = BigQueryCreateExternalTableOperator(
    #     task_id="create_bq_external_table",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": BQ_PROJECT,
    #             "datasetId": BQ_DATASET,
    #             "tableId": BQ_TABLE,
    #         },
    #         "externalDataConfiguration": {
    #             "sourceUris": [f"gs://{GCS_BUCKET}/{gcs_gz_path.replace('.gz', '.parquet')}"],
    #             "sourceFormat": "PARQUET",
    #         },
    #     },
    # )
    

    # DAG Task 執行順序
    fetch_data_task >> ingest_and_save_task
    # >> load_gcs_task >> load_bigquery_task
    
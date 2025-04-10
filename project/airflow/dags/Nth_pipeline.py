from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.decorators import task
from google.cloud import storage
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, unix_timestamp
from airflow.models import Variable
import os
import logging

# Logger 設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# GCS 和 BigQuery 設定
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCS_PATH =  Variable.get("GCS_PATH")
GCS_PROCESS_PATH = Variable.get("GCS_PROCESS_PATH")
BQ_PROJECT = Variable.get("BQ_PROJECT")
BQ_DATASET = Variable.get("BQ_DATASET")
BQ_TABLE_MOST_ACTIVE = "most_active_developers"

# Default arguments 設定
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# DAG 定義
with DAG(
    dag_id="process_most_active_developers",
    default_args=default_args,
    schedule_interval='@hourly',  # 每小時執行
    catchup=False,
) as dag:
    
    # 確保資料夾存在
    os.makedirs(f"{path_to_local_home}/activate_data", exist_ok=True)

    # 下載 GCS 上的 Parquet 檔案到本地
    def download_parquet_from_gcs(bucket_name: str, gcs_path: str, local_base_path: str, **kwargs):
        # 建立本地資料夾
        local_dir = os.path.join(local_base_path, "activate_data")
        os.makedirs(local_dir, exist_ok=True)

        # 使用 GCP SDK 下載檔案
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=gcs_path)  # 列出所有符合條件的檔案

        # 下載檔案並保存至本地
        downloaded_files  = []
        for blob in blobs:
            local_file = os.path.join(local_dir, blob.name.split('/')[-1])  # 檔案名
            try:
                logger.info(f"下載檔案: {blob.name} 到 {local_file}")
                blob.download_to_filename(local_file)
                logger.info(f"檔案下載完成: {local_file}")
                downloaded_files.append(local_file)
            except Exception as e:
                logger.error(f"檔案下載失敗: {blob.name}, 錯誤: {e}")
        
        # 使用 XCom 推送下載的檔案路徑列表
        ti = kwargs['ti']
        ti.xcom_push(key='downloaded_files', value=downloaded_files )
        return downloaded_files


    download_gcs_parquet_task = PythonOperator(
        task_id="download_gcs_parquet",
        python_callable=download_parquet_from_gcs,
        op_kwargs={
            "bucket_name": GCS_BUCKET,
            "gcs_path": GCS_PATH,
            "local_base_path": path_to_local_home
        },
        provide_context=True,  # 確保提供上下文
        do_xcom_push=True,     # 啟用 XCom 推送
    )
    
    # 使用 PySpark 進行資料清理和轉換
    def spark_cleaning_and_transformation(output_dir: str, **kwargs):
        
        input_files = kwargs['ti'].xcom_pull(task_ids='download_gcs_parquet', key='downloaded_files')
        
        for input_file in input_files:
            print(f"input_file {input_file}")

        spark = SparkSession.builder \
            .appName("GitHub Activity Data Processing") \
            .getOrCreate()

        # 計算一個月前的日期
        one_month_ago = datetime.now() - timedelta(days=30)
        output_files = []
        # 遍歷所有下載的 parquet 檔案
        for input_file in input_files:
            try:
                logger.info(f"處理檔案: {input_file}")
                df = spark.read.parquet(input_file)
                print(f"輸入範本 {df.show(5)}")

                # 清理和轉換資料
                df_cleaned = df.select(
                    col("id").alias("event_id"),
                    col("type").alias("event_type"),
                    col("actor.id").alias("actor_id"),
                    col("actor.login").alias("actor_login"),
                    col("repo.id").alias("repo_id"),
                    col("repo.name").alias("repo_name"),
                    col("created_at")
                )

                # 轉換時間戳
                df_cleaned = df_cleaned.withColumn("created_at", unix_timestamp(col("created_at")).cast("timestamp"))

                # 過濾最近一個月的資料
                df_filtered = df_cleaned.filter(col("created_at") >= one_month_ago)

                # 計算每個 Repo 的事件數量
                df_repo_activity = df_filtered.groupBy("repo_id", "repo_name") \
                    .agg(count("event_id").alias("event_count")) \
                    .orderBy(col("event_count").desc())

                # 儲存處理後的資料為 Parquet 檔案
                output_file = os.path.join(output_dir, f"repo_activity_{input_file.split('/')[-1].replace('.json.parquet', '')}")
                output_files.append(output_file)
                df_repo_activity.write.parquet(output_file, mode="overwrite")
                print(f" 輸出範本 {df_repo_activity.show(5)}")
                logger.info(f"檔案處理完成並儲存至: {output_dir}")
            except Exception as e:
                logger.error(f"處理檔案失敗: {input_file}, 錯誤: {e}")
        
        spark.stop()
        return output_files
        # 推送結果至 XCom，將 output_path_list 推送至後續任務
    
   
    spark_processing_task = PythonOperator(
        task_id="spark_cleaning_and_transformation",
        python_callable=spark_cleaning_and_transformation,
        op_kwargs={
            "output_dir": f"{path_to_local_home}/activate_data/processed_data"
        },
        provide_context=True,  # 確保提供上下文
    )
    
    # 使用 for loop 分別上傳至 GCS 和 BigQuery
    # 假設 `dag` 是任務所屬的 DAG 物件
    def upload_to_gcs_and_bigquery(ti, **kwargs):
        # 從 XCom 取得下載和處理後的檔案清單
        downloaded_files = ti.xcom_pull(task_ids='download_gcs_parquet', key='downloaded_files')
        processed_files = ti.xcom_pull(task_ids='spark_cleaning_and_transformation', key='return_value')

        # 對每個檔案進行上傳處理
        for file in processed_files:
            gcs_folder = f"{GCS_PROCESS_PATH}/repo_activity/{os.path.basename(file)}"
            print(f"src {file}")
            print(f"gcs_folder {gcs_folder}")
            for gcs_file in file:
                
                # 上傳檔案到 GCS
                upload_to_gcs = LocalFilesystemToGCSOperator(
                    task_id=f"upload_to_gcs_{os.path.basename(file)}_{gcs_file}",
                    src=file,
                    dst=gcs_file,
                    bucket=GCS_BUCKET,
                    dag=kwargs['dag']  # 確保任務關聯到正確的 DAG
                )

                # 上傳檔案從 GCS 到 BigQuery
                upload_to_bq = GCSToBigQueryOperator(
                    task_id=f"upload_to_bq_{os.path.basename(file)}_{gcs_file}",
                    bucket=GCS_BUCKET,
                    source_objects=[gcs_file],
                    destination_project_dataset_table=f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE_MOST_ACTIVE}",
                    schema_fields=[
                        {'name': 'event_id', 'type': 'STRING'},
                        {'name': 'event_type', 'type': 'STRING'},
                        {'name': 'actor_id', 'type': 'STRING'},
                        {'name': 'actor_login', 'type': 'STRING'},
                        {'name': 'repo_id', 'type': 'STRING'},
                        {'name': 'repo_name', 'type': 'STRING'},
                        {'name': 'created_at', 'type': 'TIMESTAMP'}
                    ],
                    source_format="PARQUET",
                    write_disposition='WRITE_APPEND',
                    dag=kwargs['dag']  # 確保任務關聯到正確的 DAG
                )

                # 確保 GCS 上傳完成後再進行 BigQuery 上傳
                upload_to_gcs >> upload_to_bq
        
        
    upload_to_gcs_and_bigquery_task = PythonOperator(
        task_id="upload_to_gcs_and_bigquery",
        python_callable=upload_to_gcs_and_bigquery,
        provide_context=True,  # 確保提供上下文
    )
        
        # 任務順序
    download_gcs_parquet_task >> spark_processing_task  >> upload_to_gcs_and_bigquery_task

    # >> load_to_bigquery_task

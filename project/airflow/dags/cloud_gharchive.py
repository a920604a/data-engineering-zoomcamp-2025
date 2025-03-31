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
import os
import logging
import psycopg2

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


# PostgreSQL 連線配置
DB_CONN = {
    "dbname": "zoomcamp",
    "user": "zoomcamp",
    "password": "zoomcamp",
    "host": "postgres-dz",
    "port": "5432",
}

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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
    download_dataset_task = BashOperator(
        task_id="download_dataset",
        bash_command=f"wget {dataset_url} -O {local_gz_path}"
    )

    # 上傳 .json.gz 到 Google Cloud Storage
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=local_gz_path,
        dst=gcs_gz_path,
        bucket=GCS_BUCKET,
        mime_type="application/gzip"
    )

    # 從 GCS 下載 .json.gz
    download_from_gcs_task = GCSToLocalFilesystemOperator(
        task_id="download_from_gcs",
        bucket=GCS_BUCKET,
        object_name=gcs_gz_path,
        filename=local_gz_path
    )

    # # 解壓縮 .gz 檔案
    def extract_gz_file():
        with gzip.open(local_gz_path, "rt", encoding="utf-8") as f_in, open(local_json_path, "w", encoding="utf-8") as f_out:
            f_out.write(f_in.read())
        logger.info(f"解壓縮完成: {local_json_path}")

    extract_task = PythonOperator(
        task_id="extract_gz",
        python_callable=extract_gz_file
    )

    

    create_bq_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET,
        project_id=BQ_PROJECT,
        location=BQ_DATASET_AREA,  # 需要與你的 BigQuery 設定相符
    )


    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource={
            "tableReference": {
                "projectId": "dz-final-project",
                "datasetId": "gharchive",
                "tableId": "watch_events_external_table",
            },
            "externalDataConfiguration": {
                "sourceUris": ["gs://dz-data-lake/gharchive/*.json.gz"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "compression": "GZIP",
                "schema": {
                    "fields": [
                    {"name": "id", "type": "STRING"},
                    {"name": "type", "type": "STRING"},
                    {"name": "actor", "type": "RECORD", "fields": [
                        {"name": "id", "type": "STRING"},
                        {"name": "login", "type": "STRING"},
                        {"name": "display_login", "type": "STRING"},
                        {"name": "gravatar_id", "type": "STRING"},
                        {"name": "url", "type": "STRING"},
                        {"name": "avatar_url", "type": "STRING"},
                    ]},
                    {"name": "repo", "type": "RECORD", "fields": [
                        {"name": "id", "type": "STRING"},
                        {"name": "name", "type": "STRING"},
                        {"name": "url", "type": "STRING"},
                    ]},
                    {"name": "payload", "type": "RECORD", "fields": [
                        {"name": "repository_id", "type": "STRING"},
                        {"name": "push_id", "type": "STRING"},
                        {"name": "size", "type": "INTEGER"},
                        {"name": "distinct_size", "type": "INTEGER"},
                        {"name": "ref", "type": "STRING"},
                        {"name": "head", "type": "STRING"},
                        {"name": "before", "type": "STRING"},
                        {"name": "commits", "type": "RECORD", "fields": [
                            {"name": "sha", "type": "STRING"},
                            {"name": "author", "type": "RECORD", "fields": [
                                {"name": "email", "type": "STRING"},
                                {"name": "name", "type": "STRING"},
                            ]},
                            {"name": "message", "type": "STRING"},
                            {"name": "distinct", "type": "BOOLEAN"},
                            {"name": "url", "type": "STRING"},
                        ]},
                    ]},
                    {"name": "public", "type": "BOOLEAN"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                ]
                },
             "ignoreUnknownValues": True,  # 忽略未知欄位
            },
        },
    )


    # # 將解壓縮的 JSON 存入 BigQuery
    insert_into_bigquery_task = BigQueryInsertJobOperator(
        task_id="insert_into_bigquery",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
                    PARTITION BY DATE(created_at)
                    AS
                    SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_external_table`
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )


    # # 刪除本地解壓縮的 JSON 檔案
    def delete_local_json():
        if os.path.exists(local_json_path):
            os.remove(local_json_path)
            logger.info(f"已刪除本地 JSON 文件: {local_json_path}")

    delete_json_task = PythonOperator(
        task_id="delete_local_json",
        python_callable=delete_local_json
    )

    # # 過濾 `WatchEvent` 並存入 PostgreSQL
    # def process_watch_events():
    #     with open(local_json_path, "r", encoding="utf-8") as f:
    #         events = [json.loads(line) for line in f]

    #     watch_events = [
    #         (event["id"], event["repo"]["name"], event["public"], event["created_at"])
    #         for event in events if event.get("type") == "WatchEvent"
    #     ]

    #     if not watch_events:
    #         logger.info("No WatchEvent found.")
    #         return

    #     conn = psycopg2.connect(**DB_CONN)
    #     cur = conn.cursor()
    #     sql = """
    #     INSERT INTO watch_events (id, repo_name, public, created_at) 
    #     VALUES (%s, %s, %s, %s)
    #     ON CONFLICT (id) DO NOTHING;
    #     """
        
    #     cur.executemany(sql, watch_events)
    #     conn.commit()
    #     cur.close()
    #     conn.close()
    #     logger.info(f"{len(watch_events)} WatchEvent(s) inserted into DB.")

    # process_task = PythonOperator(
    #     task_id="process_watch_events",
    #     python_callable=process_watch_events
    # )
    
    # analyze_task = BashOperator(
    #     task_id="analyze_stars",
    #     bash_command="spark-submit --master local[*] /opt/airflow/scripts/analyze_stars.py"
    # )

    # DAG Task 執行順序
    download_dataset_task >> upload_to_gcs_task >> download_from_gcs_task
    download_from_gcs_task >> extract_task >> create_bq_dataset_task >> bigquery_external_table_task >> insert_into_bigquery_task >> delete_json_task
    # extract_task >> process_task >> analyze_task

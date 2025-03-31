from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import gzip
import json
import psycopg2
import os
import logging

# 設定 Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# PostgreSQL 連線配置
DB_CONN = {
    "dbname": "zoomcamp",
    "user": "zoomcamp",
    "password": "zoomcamp",
    "host": "postgres-dz",
    "port": "5432",
}

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")


# 設定 DAG 參數
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_gharchive_dag",
    default_args=default_args,
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

    # 確保資料夾存在
    os.makedirs(f"{path_to_local_home}/data", exist_ok=True)

    # 下載 GH Archive 數據
    download_dataset_task = BashOperator(
        task_id="download_dataset",
        bash_command=f"wget {dataset_url} -O {local_gz_path}"
    )

    # 解壓縮 .gz 檔案
    def extract_gz_file():
        with gzip.open(local_gz_path, "rt", encoding="utf-8") as f_in, open(local_json_path, "w", encoding="utf-8") as f_out:
            f_out.write(f_in.read())
        logger.info(f"解壓縮完成: {local_json_path}")

    extract_task = PythonOperator(
        task_id="extract_gz",
        python_callable=extract_gz_file
    )

    # 過濾 `WatchEvent` 並存入 PostgreSQL
    def process_watch_events():
        with open(local_json_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f]

        watch_events = [
            (event["id"], event["repo"]["name"], event["public"], event["created_at"])
            for event in events if event.get("type") == "WatchEvent"
        ]

        if not watch_events:
            logger.info("No WatchEvent found.")
            return

        conn = psycopg2.connect(**DB_CONN)
        cur = conn.cursor()
        sql = """
        INSERT INTO watch_events (id, repo_name, public, created_at) 
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        
        
        cur.executemany(sql, watch_events)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(watch_events)} WatchEvent(s) inserted into DB.")

    process_task = PythonOperator(
        task_id="process_watch_events",
        python_callable=process_watch_events
    )
    
    

    # DAG Task 執行順序
    download_dataset_task >> extract_task >> process_task
    
    
    

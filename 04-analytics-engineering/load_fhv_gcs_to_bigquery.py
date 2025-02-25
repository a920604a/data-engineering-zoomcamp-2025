from google.cloud import storage, bigquery
import re
import os

# 配置
project_id = 'keen-dolphin-450409-m8'
dataset_id = 'trips_data_all'
bucket_name = 'dtc-data-lake-tim'  # 替換為你的 GCS Bucket 名稱
prefix = 'fhv_tripdata_2019-'  # 文件前綴
file_pattern = re.compile(r'fhv_tripdata_2019-\d{2}\.csv\.gz')  # 文件名正則表達式
json_file = "/home/tim/.gcp/google_credentials.json"  # 替換為你的服務帳戶金鑰文件路徑

# 設置環境變數
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_file

# 初始化客戶端
storage_client = storage.Client()
bigquery_client = bigquery.Client(project=project_id)

schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("PUlocationID", "INTEGER"),
    bigquery.SchemaField("DOlocationID", "INTEGER"),
    bigquery.SchemaField("SR_Flag", "INTEGER"),  # 確保類型一致
    bigquery.SchemaField("Affiliated_base_number", "STRING"),
]

def list_gcs_files(bucket_name, prefix):
    """
    列出 GCS 中符合前綴的文件
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if file_pattern.match(blob.name)]
    return files

def load_gcs_to_bigquery(uri, table_id):
    """
    將 GCS 文件加載到 BigQuery
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # 跳過標題行
        schema=schema,  # 使用手動定義的 schema
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 追加到現有表格
    )

    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()  # 等待任務完成
    print(f"Loaded {uri} into {table_id}")

def main():
    # 列出 GCS 文件
    files = list_gcs_files(bucket_name, prefix)
    if not files:
        print("No files found in GCS.")
        return

    # 設定目標表格 ID
    destination_table_id = f"{project_id}.{dataset_id}.fhv_tripdata"

    # 加載每個文件到同一張 BigQuery 表格
    for file in files:
        # GCS URI
        uri = f"gs://{bucket_name}/{file}"

        # 加載到 BigQuery
        print(f"Loading {uri} into {destination_table_id}...")
        try:
            load_gcs_to_bigquery(uri, destination_table_id)
        except Exception as e:
            print(f"Failed to load {uri}: {e}")

if __name__ == "__main__":
    main()
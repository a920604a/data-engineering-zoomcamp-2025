import os
import time
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage

# 設定 GCS bucket 和本地資料夾路徑
bucket_name = 'dtc-data-lake-tim'
local_directory = '/Users/chenyuan/Downloads/nyc_taxi_data'  # 本地資料夾的路徑

# 初始化 Google Cloud Storage 客戶端
CREDENTIALS_FILE = "/Users/chenyuan/Downloads/google_credentials.json"  
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)


# 取得 GCS bucket 物件
bucket = client.get_bucket(bucket_name)

def upload_to_gcs(local_file):
    """將單個檔案上傳到 GCS"""
    try:
        blob = bucket.blob(local_file)
        blob.upload_from_filename(os.path.join(local_directory, local_file))
        print(f'{local_file} 上傳完成!')
    except Exception as e:
        print(f'上傳 {local_file} 時發生錯誤: {e}')

def main():
    # 取得所有檔案清單
    
    files_to_upload = [f for f in os.listdir(local_directory) if os.path.isfile(os.path.join(local_directory, f))]
    
    # 設定最大執行緒數量
    max_workers = 5
    
    # 使用 ThreadPoolExecutor 來進行併發上傳
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(upload_to_gcs, files_to_upload)
    
    end_time = time.time()
    print(f'所有檔案上傳完成，總共花費 {end_time - start_time:.2f} 秒')

if __name__ == '__main__':
    main()

import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

# 設定 GCS bucket 和本地資料夾路徑
bucket_name = 'dtc-data-lake-tim'
json_file =  "/home/tim/.gcp/google_credentials.json"  

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_file
# Initialize a storage client
storage_client = storage.Client()
print("Storage client initialized.")
# Get your bucket
bucket = storage_client.get_bucket(bucket_name)
print(f"Bucket {bucket_name} accessed.")
max_workers = 10  # 最大線程數


base_urls = {
    'yellow': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_',
    'green': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_',
    'fhv': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_'
}

years = ['2019', '2020']
months = [str(month).zfill(2) for month in range(1, 13)]


def download_and_upload(taxi_type, year, month):
    """
    下載數據並上傳到 GCS
    """
    if taxi_type == 'fhv' and year == '2020':
        print(f"Skipping FHV data for year {year}")
        return

    url = f"{base_urls[taxi_type]}{year}-{month}.csv.gz"
    try:
        print(f"Fetching data from: {url}")
        response = requests.get(url, timeout=500)
        response.raise_for_status()  # 檢查 HTTP 狀態碼
        blob = bucket.blob(f"{taxi_type}_tripdata_{year}-{month}.csv.gz")
        blob.upload_from_string(response.content)
        print(f"Successfully uploaded {taxi_type}_tripdata_{year}-{month}.csv.gz to GCS")
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
    except Exception as e:
        print(f"An error occurred while processing {url}: {e}")


def download_and_upload(taxi_type, year, month):
    """
    下載數據並上傳到 GCS
    """
    if taxi_type == 'fhv' and year == '2020':
        print(f"Skipping FHV data for year {year}")
        return

    url = f"{base_urls[taxi_type]}{year}-{month}.csv.gz"
    try:
        print(f"Fetching data from: {url}")
        response = requests.get(url, timeout=500)
        response.raise_for_status()  # 檢查 HTTP 狀態碼
        blob = bucket.blob(f"{taxi_type}_tripdata_{year}-{month}.csv.gz")
        blob.upload_from_string(response.content)
        print(f"Successfully uploaded {taxi_type}_tripdata_{year}-{month}.csv.gz to GCS")
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
    except Exception as e:
        print(f"An error occurred while processing {url}: {e}")
        
def main():
    # 創建任務列表
    tasks = []
    for taxi_type, base_url in base_urls.items():
        for year in years:
            for month in months:
                tasks.append((taxi_type, year, month))

    # 使用 ThreadPoolExecutor 並行處理
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_and_upload, *task) for task in tasks]
        for future in as_completed(futures):
            future.result()  # 等待任務完成並處理異常


if __name__ == '__main__':
    main()

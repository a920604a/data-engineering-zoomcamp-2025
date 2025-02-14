import pandas as pd
from sqlalchemy import create_engine


df = pd.read_csv('green_tripdata_2019-10.csv', nrows=100)
engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')

print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))


from time import time
df_iter = pd.read_csv('green_tripdata_2019-10.csv', iterator=True, chunksize=10000)

while True: 
    try:
        t_start = time()

        # 讀取一個數據塊
        df = next(df_iter)

        # 處理日期格式
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        # 插入數據到資料庫
        df.to_sql(name='green_taxi_data', con=engine, if_exists='append', index=False)

        t_end = time()

        print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')

    except StopIteration as e:
        print(f"All data has been processed. {e}")
        break
   

df_zones = pd.read_csv('taxi_zone_lookup.csv')
df_zones.to_sql(name='zones', con=engine, if_exists='replace')

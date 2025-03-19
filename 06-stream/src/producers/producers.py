import json
import pandas as pd
import time
import concurrent.futures
import threading
from kafka import KafkaProducer

MAX_WORKERS = 64
CHUNKSIZE = 2500
TOPIC_NAME = "green-trips" 
producer = None

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def init_producer():
    global producer
    server = 'localhost:9092'

    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer
    )

    return producer.bootstrap_connected()


def sent_data(data_raw, chunk_number):
    thread_id = threading.current_thread().name
    print(f"[Thread {thread_id}] - Sending chunk number {chunk_number} with {len(data_raw)} records...")

    for message in data_raw:
        producer.send(TOPIC_NAME, value=message)
        time.sleep(0.05)

    producer.flush()
    print(f"[Thread {thread_id}] - Chunk number sending completed {chunk_number}.")


def get_data_raw():
    
    columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']
    topic_name = "green-trips"

    t0 = time.time()      
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        for idx, chunk in enumerate(pd.read_csv("../../data/green_tripdata_2019-10.csv", chunksize=CHUNKSIZE)):
            chunk = chunk[columns]
            chunk.loc[:, columns[4]] = chunk[columns[4]].fillna(0)
            data_dict = chunk.to_dict(orient='records')
            futures.append(executor.submit(sent_data, data_dict, idx + 1))
        
        concurrent.futures.wait(futures)

    t1 = time.time()
    took = t1 - t0
    print(f"\nTiempo total de ejecución: {took:.2f} segundos")


if __name__ == '__main__':

    if init_producer():
        get_data_raw()
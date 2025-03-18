from time import time
import pandas as pd
import csv
import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

df = pd.read_csv('green_tripdata_2019-10.csv.gz')

print(df.head(1))



df = df[['lpep_pickup_datetime',
'lpep_dropoff_datetime',
'PULocationID',
'DOLocationID',
'passenger_count',
'trip_distance',
'tip_amount']]

df.to_csv('green_tripdata_2019-10.csv', index=False)


csv_file = 'green_tripdata_2019-10.csv'  # change to your CSV file path if needed
t0 = time()
with open(csv_file, 'r', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        # Each row will be a dictionary keyed by the CSV headers
        # Send data to Kafka topic "green-trips"
        producer.send('green-trips', value=row)
        
producer.flush()
t1 = time()

print(f'took {(t1 - t0):.2f} seconds')

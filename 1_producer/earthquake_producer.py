from confluent_kafka import Producer
import requests
import json 
import time


def error_callback(err):
    print(f"Error: {err}")

# Setting Kafka configurations and retry attempts
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'socket.timeout.ms': 60000,
    'request.timeout.ms': 30000,
    'retries': 5,
    'retry.backoff.ms': 500,
    'queue.buffering.max.ms': 1000,
    'error_cb': error_callback
}


producer = Producer(producer_config)

#USGS Earthquake API URL
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

while True:

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Sending the fetched data to the Kafka 'earthquake' topic
            producer.produce('earthquake', json.dumps(data)) #Topic creation
            print("Data fetched and sent to Kafka")         
        
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}") 
        # Ensuring that the messages are sent to Kafka
        producer.flush()
        #The data will be fetched every 30 minutes.
        time.sleep(1800)

    except Exception as e:
        print(f"Exception: {e}")


# Kafka Data Streaming

#### Objective
The main objective of Part 1 is to stream earthquake data in near real-time by utilizing a Kafka producer to fetch data from the USGS API. This data is then consumed by a Kafka consumer, processed using PySpark, and prepared for further handling and storage.

#### Key Features
- **Kafka Producer**: Retrieves earthquake data from the USGS API at 30-minute intervals, while also handling real-time events as they occur.
- **Kafka Consumer**: Consumes the streamed data and processes it using PySpark, preparing it for transformation and storage.

#### Architecture Overview
1. **Producer**: Connects to the USGS Earthquake API and streams the JSON data to a Kafka topic.
2. **Consumer**: Uses PySpark to consume the Kafka topic data and prepares it for further transformation.

#### Setup Instructions

1. **Kafka Setup**: Ensure you have a running Kafka instance and a topic named `earthquake`.
   
2. **Config.json**:  
   A JSON file template containing credentials, connections, and other details.

3. **Install Dependencies**:
   - **Kafka**:
     ```bash
     pip install kafka-python
     ```
   - **PySpark**:
     ```bash
     pip install pyspark==3.5.2
     ```
   - **Confluent Kafka Python client**

4. **Producer Configuration**:  
   Set up the producer to fetch data from the USGS API and push it to the Kafka topic. The data is retrieved in JSON format.

5. **Consumer Configuration**:  
   Configure a Kafka consumer to pull data from the topic and process it with PySpark.

#### Future Enhancements
In **Part 2**, the data will be ingested into Snowflake for transformation and analysis. Tasks will be set up in Snowflake to handle data parsing, transformation, and distribution into fact and dimension tables.

---

### Producer Code Overview

The Kafka producer in this project is responsible for fetching earthquake data from the USGS API and streaming it to a Kafka topic. Here's a breakdown of the code:

#### 1. **Dependencies**:
   - **Confluent Kafka Python Client** (`confluent_kafka`): Used to produce messages to a Kafka topic.
   - **Requests** (`requests`): Used to fetch earthquake data from the USGS API.
   - **JSON**: Used for parsing and encoding the fetched data into JSON format.
   - **Time**: Used to control the interval between successive API requests (set to 30 minutes).

#### 2. **Error Handling**:
   A callback function `error_callback` is defined to print any errors encountered during the Kafka message production process.
   ```python
    def error_callback(err):
        print(f"Error: {err}")
```

#### 3. **Producer Configuration**:
   The `producer_config` dictionary holds key configuration parameters:
   - `bootstrap.servers`: The address of the Kafka broker.
   - `socket.timeout.ms`, `request.timeout.ms`: Timeout settings for communication.
   - `retries`, `retry.backoff.ms`: Retry configuration for transient failures.
   - `queue.buffering.max.ms`: Maximum buffering time for messages before sending.
```python
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'socket.timeout.ms': 60000,
    'request.timeout.ms': 30000,
    'retries': 5,
    'retry.backoff.ms': 500,
    'queue.buffering.max.ms': 1000,
    'error_cb': error_callback
}
```


#### 4. **Producer Initialization**:
   The `Producer` object is initialized with the configuration parameters, which is then used to send messages to the Kafka topic `earthquake`.
   ```python
    producer = Producer(producer_config)
  ```

#### 5. **Fetching and Sending Data**:
   - The code repeatedly makes GET requests to the USGS Earthquake API to fetch the latest earthquake data in GeoJSON format.     
   - If the request is successful (`status_code == 200`), the data is parsed and converted to a JSON string before being sent to the Kafka topic `earthquake`.
   - The producer’s `flush` method is called to ensure that all messages are sent before the program sleeps for 30 minutes (`time.sleep(1800)`).

     ```python
     while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                producer.produce('earthquake', json.dumps(data))
                print("Data fetched and sent to Kafka")         
            
         else:
             print(f"Failed to retrieve data. Status code: {response.status_code}") 
    
            producer.flush()
            time.sleep(1800)
      ```
     

#### 6. **Error Handling and Retry**:
   - If the request fails or any exception occurs, the error is caught and printed for debugging purposes.
     ```python
         except Exception as e:
        print(f"Exception: {e}")
     ```

---

### Key Points:
- The producer runs in an infinite loop, ensuring that data is fetched and sent to Kafka at regular intervals.
- The system is designed to handle intermittent errors with retries and timeouts.
- The producer is set to fetch data every 30 minutes, though real-time earthquake events may be available in between.

This producer script is the first part of the pipeline, responsible for streaming earthquake data to Kafka for further processing.

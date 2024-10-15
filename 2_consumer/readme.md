# Kafka to Snowflake Streaming Consumer

This project demonstrates how to stream earthquake data from a Kafka topic, process it using Apache Spark, and write the results to a Snowflake data warehouse. The application uses PySpark to handle Kafka streams and the Snowflake Spark connector to store the processed data.

> **Note**: A 30-day free trial of Snowflake was used for this project.

## Objective

The primary goal of this project is to consume earthquake data in real-time from a Kafka topic, process the data with Apache Spark, and store the processed data into Snowflake for further analysis.


## Key Features

- **Kafka Streaming**: Consumes real-time earthquake data from a Kafka topic.
- **Spark Processing**: Uses PySpark to process the streamed data.
- **Snowflake Integration**: Stores the processed data into a Snowflake table for further analysis.

## Requirements

Before running the code, make sure you have the following dependencies installed:

### Python Libraries
- **PySpark**: The main library for processing streaming data with Apache Spark.
- **JSON**: Used to load configuration settings.

You can install the required Python dependencies by running:

```bash
pip install pyspark==3.4.0
```
> **Note**: According to [official documentation](https://docs.snowflake.com/en/user-guide/spark-connector), "Snowflake supports three versions of Spark: Spark 3.2, Spark 3.3, and Spark 3.4. There is a separate version of the Snowflake connector for each version of Spark. Use the correct version of the connector for your version of Spark."


## Additional Requirements

- **Kafka**: You need a running Kafka instance and a topic where earthquake data is being streamed (e.g., `earthquake`).
- **Snowflake**: Make sure you have a Snowflake account with the necessary credentials and a table to store the data.

## Necessary JARs for Spark

The following JAR files are required for integrating Spark with Kafka and Snowflake. You can add them to the `config.json` file (detailed below) or download them manually:

- **Kafka JAR**: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
- **Snowflake JAR**: `net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4`

## config.json

A configuration file (`config.json`) is required for connecting to Kafka and Snowflake. It contains necessary information like Kafka broker details, Snowflake credentials, and paths to the required JARs.

> **Note**: In the `config.json`, you need to specify the **schema** and **table** for Snowflake. However, the table doesn't need to be created beforehand. When writing data to Snowflake, the platform will automatically create a new table in the specified schema to accommodate the receiving data.

### Example `config.json`:

```json
{
  "spark_session": {
    "kafka_jars": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "snowflake_jars": "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4"
  },
  "kafka_information": {
    "host": "localhost:9092",
    "topic": "earthquake"
  },
  "snowflake": {
    "sfURL": "your_snowflake_url",
    "sfUser": "your_snowflake_user",
    "sfPassword": "your_snowflake_password",
    "sfDatabase": "your_snowflake_database",
    "sfSchema": "your_snowflake_schema",
    "sfWarehouse": "your_snowflake_warehouse",
    "sfJdbcUrl": "your_snowflake_jdbc_url",
    "dbtable": "your_snowflake_table"
  }
}
```
## Code Explanation

### 1. Class: `consume_data`

- **`__init__(self, config_file)`**: Initializes the class by reading the configuration from a JSON file. It sets up the Spark session with the necessary Kafka and Snowflake JARs, and configures the connection to Snowflake.

- **`process_streaming_data(self)`**: Reads streaming data from Kafka, processes it as a DataFrame, and selects the relevant data.

- **`write_to_snowflake(self, batch_df, batch_id)`**: Writes the processed data (in batches) to a Snowflake table. It shows the schema and the contents of the DataFrame before appending to Snowflake.

- **`start_streaming(self, final_df)`**: Starts the stream, continuously writing the transformed data to Snowflake.

### 2. Main Execution:
The script initiates a `consume_data` object with the path to the configuration file (`config.json`), processes the streaming data, and begins the streaming query to Snowflake.

## Running the Code

1. **Ensure Kafka and Snowflake are running**:
   - Kafka should be up and running, with the topic `earthquake` containing earthquake data.
   - You need a Snowflake account and a table where the data will be stored.

2. **Create or update the `config.json` file** with your Kafka and Snowflake connection details.

3. **Execute the script**:

```bash
python consumer.py
```

import pyspark
from pyspark.sql import SparkSession
import json

print(pyspark.__version__) # Check if PySpark version is 3.4, as supported by Snowflake


class consume_data:
        
        def __init__(self, config_file):                
                              
                with open(config_file, 'r') as f:                       
                       config = json.load(f)    
                
                #Define config.json variables
                self.spark_session = config['spark_session']
                self.kafka_information = config['kafka_information']
                self.snowflake = config['snowflake']            
               
               
               
                self.spark = SparkSession.builder \
                                .appName("Earthquake_data") \
                                .config("spark.jars.packages", self.spark_session['kafka_jars'] +','+ self.spark_session['snowflake_jars'])\
                                #Append Snowflake session credentials in Spark Session
                                .config("spark.snowflake.sfurl", self.snowflake['sfURL'])\
                                .config("spark.snowflake.sfUser", self.snowflake['sfUser'])\
                                .config("spark.snowflake.sfPassword", self.snowflake['sfPassword'])\
                                .config("spark.snowflake.sfDatabase", self.snowflake['sfDatabase'])\
                                .config("spark.snowflake.sfSchema", self.snowflake['sfSchema'])\
                                .config("spark.snowflake.sfWarehouse", self.snowflake['sfWarehouse'])\
                                .config("spark.snowflake.sfJdbcurl", self.snowflake['sfJdbcUrl'])\
                                .config("spark.sql.debug", "true")\
                                .getOrCreate()

                
        def process_streaming_data(self):
               
               input_df = self.spark.readStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", self.kafka_information['host'])\
                .option("subscribe", self.kafka_information['topic'])\
                .load()
               
               # Select relevant data from incoming Kafka stream (as JSON string)
               final_df = input_df.selectExpr("CAST(value as STRING) AS json_value")
                            
               
               return final_df
        
            
        def write_to_snowflake(self, batch_df, batch_id):
            
            # Debugging: Print schema and data being written to Snowflake
            batch_df.printSchema() # Print schema to console       
            batch_df.show() # Print data to console
                
            batch_df.write\
                .format("net.snowflake.spark.snowflake")\
                .option("sfURL", self.snowflake['sfURL']) \
                .option("sfDatabase", self.snowflake['sfDatabase']) \
                .option("sfSchema", self.snowflake['sfSchema']) \
                .option("sfWarehouse", self.snowflake['sfWarehouse']) \
                .option("sfUser", self.snowflake['sfUser']) \
                .option("sfPassword", self.snowflake['sfPassword']) \
                .option("dbtable", self.snowflake['dbtable'])\
                .mode("append") \
                .save()
            
        
        
        def start_streaming(self, final_df):

        # Write transformed dataframe (final_df) to Snowflake in streaming mode  
            streamingQuery = final_df.writeStream\
                    .foreachBatch(self.write_to_snowflake)\
                    .outputMode("append")\ #Every new data batch will be appended to Snowflake
                    .start()
            

                
            streamingQuery.awaitTermination()


    
config_file="path/to/config.json"
run = consume_data(config_file)
final_df = run.process_streaming_data()
run.start_streaming(final_df)

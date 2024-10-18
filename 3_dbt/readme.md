## Data Loading Process

The data is ingested into Snowflake through a Spark consumer, which writes to an initial staging table created by the Spark task. The data arrives in Snowflake in the form of JSON rows, which must be parsed to create a columnar version of the staging table for easier querying and transformations.

### Reason for Using a New Staging Table

To ensure safe debugging without compromising the integrity of the data, a second staging table was introduced. During the development and testing phases of the Kafka and Spark jobs, debugging required multiple iterations, including the deletion of test data. To avoid potential data loss during these tests, the new staging table serves as a temporary transformed version of the raw JSON data. This approach guarantees that the original data remains intact while allowing for effective testing and debugging.

### Purpose of the New Staging Table

In addition to supporting debugging, the new staging table also provides an intermediate step to apply transformations that are difficult to perform directly on JSON data. By converting the data into a columnar format early in the pipeline, we ensure that important transformations are not overlooked and that the data is fully prepared for loading into the final schema tables.

### Task Automation

The process is automated through Snowflake tasks. Specifically, `task_1` runs every 30 minutes, in sync with the scheduled jobs in Spark and Kafka, ensuring timely data ingestion and processing. Once `task_1` is complete, additional tasks are triggered to follow up and continue the data pipeline, applying further transformations and finally loading the cleaned data into the schema tables.

### Final Data Processing

After the transformations in the staging table are complete, the cleaned and structured data is moved into the schema tables, where it becomes available for further analysis and reporting.

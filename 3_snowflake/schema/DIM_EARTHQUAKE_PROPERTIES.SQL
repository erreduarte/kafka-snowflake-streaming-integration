CREATE TABLE IF NOT EXISTS DIM_EARTHQUAKE_PROPERTIES (
    earthquake_id STRING PRIMARY KEY,
    registered_at TIMESTAMP_NTZ,
    type STRING,
    title STRING,
    url STRING,
    detail STRING,
    net STRING,
    sources STRING,
    types STRING,
    magType STRING,
    FOREIGN KEY (earthquake_id) REFERENCES Earthquake_Facts(earthquake_id)
);
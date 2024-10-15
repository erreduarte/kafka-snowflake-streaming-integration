CREATE TABLE IF NOT EXISTS Earthquake_Location (
    earthquake_id STRING PRIMARY KEY,
    registered_at TIMESTAMP_NTZ,
    place STRING,
    geo_type STRING,
    latitude FLOAT,
    longitude FLOAT,
    altitude FLOAT,
    FOREIGN KEY (earthquake_id) REFERENCES Earthquake_Facts(earthquake_id)
);
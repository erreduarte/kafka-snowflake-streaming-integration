CREATE TABLE IF NOT EXISTS DIM_Earthquake_Status (
    earthquake_id STRING PRIMARY KEY,
    registered_at TIMESTAMP_NTZ,
    alert STRING,
    status STRING,
    felt STRING,
    cdi STRING,
    mmi STRING,
    FOREIGN KEY (earthquake_id) REFERENCES Earthquake_Facts(earthquake_id)
);
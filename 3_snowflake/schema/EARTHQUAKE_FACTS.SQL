CREATE TABLE IF NOT EXISTS Earthquake_Facts (
    earthquake_id STRING PRIMARY KEY,
    registered_at TIMESTAMP_NTZ, 
    magnitude FLOAT,
    tsunami BIGINT,
    sig BIGINT,
    nst BIGINT,
    dmin FLOAT,
    rms FLOAT,
    gap BIGINT
);

CREATE OR REPLACE TASK SEND_DATA_FACT_TABLE
WAREHOUSE = COMPUTE_WH
AFTER INSERT_ROWS_STG_CLEANED
AS
BEGIN
    -- Insert data into EARTHQUAKE_FACTS from CLEANED_EARTHQUAKE_HISTORY
    INSERT INTO 
        OPEN_WEATHER_EARTHQUAKE.EARTHQUAKE_FACTS  -- Fully qualified target table (schema.table)
    SELECT 
        EARTHQUAKE_ID, REGISTERED_AT, MAGNITUDE, TSUNAMI, SIG, NST, DMIN, RMS, GAP
    FROM (
            SELECT 
                CODE AS EARTHQUAKE_ID,
                REGISTERED_AT,
                MAGNITUDE,
                TSUNAMI,
                SIG,
                NST,
                DMIN,
                RMS,
                GAP
            FROM
                PUBLIC.CLEANED_EARTHQUAKE_HISTORY  -- Fully qualified source table (schema.table)
    ) A1
    WHERE NOT EXISTS(
        SELECT 1 
        FROM 
            OPEN_WEATHER_EARTHQUAKE.EARTHQUAKE_FACTS A2  -- Fully qualified target table (schema.table)
        WHERE 
            A1.EARTHQUAKE_ID = A2.EARTHQUAKE_ID
        AND
            A1.REGISTERED_AT = A2.REGISTERED_AT
    );

END;
CREATE OR REPLACE TASK POPULATE_DIM_PROPERTIES
WAREHOUSE = COMPUTE_WH
AFTER SEND_DATA_FACT_TABLE
AS
BEGIN
    INSERT INTO 
        OPEN_WEATHER_EARTHQUAKE.DIM_EARTHQUAKE_PROPERTIES
    SELECT * FROM (
                    SELECT 
                        CODE AS EARTHQUAKE_ID,
                        REGISTERED_AT,
                        TYPE,
                        TITLE,
                        URL,
                        DETAIL,
                        NET,
                        SOURCES,
                        TYPES,
                        MAGTYPE
                    FROM
                        PUBLIC.CLEANED_EARTHQUAKE_HISTORY) A1
    WHERE NOT EXISTS
        (SELECT
            1
         FROM
            OPEN_WEATHER_EARTHQUAKE.DIM_EARTHQUAKE_PROPERTIES A2
        WHERE
            A1.EARTHQUAKE_ID = A2.EARTHQUAKE_ID
        AND
            A1.REGISTERED_AT = A2.REGISTERED_AT
        );
END;
WITH SOURCE_DATA AS (
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
        {{ source('stg_usgs', 'STG_EARTHQUAKE_HISTORY') }}
)

SELECT * 
FROM SOURCE_DATA AS A1    
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} A2  
    WHERE 
        A1.EARTHQUAKE_ID = A2.EARTHQUAKE_ID
        AND A1.REGISTERED_AT = A2.REGISTERED_AT
)

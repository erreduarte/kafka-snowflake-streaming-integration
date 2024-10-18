WITH SOURCE_DATA AS (
    SELECT 
        ID,
        REGISTERED_AT,
        ALERT,
        STATUS,
        FELT,
        CDI,
        MMI
    FROM
        {{ source('stg_usgs', 'STG_EARTHQUAKE_HISTORY') }}
)

SELECT *
FROM SOURCE_DATA A1
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} A2
    WHERE A1.ID = A2.ID
      AND A1.REGISTERED_AT = A2.REGISTERED_AT
)

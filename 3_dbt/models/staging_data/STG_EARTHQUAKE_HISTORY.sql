WITH source_data AS (
                        SELECT
                            ROUND(f.value:properties:mag::float, 2) AS magnitude,
                            f.value:properties:place::text AS place,
                            TO_TIMESTAMP(CAST(f.value:properties:time AS NUMBER) / 1000) AS registered_at,
                            TO_TIMESTAMP(CAST(f.value:properties:updated AS NUMBER) / 1000) AS updated_at,
                            f.value:properties:tz::varchar AS tz,
                            f.value:properties:url::varchar AS url,
                            f.value:properties:detail::varchar AS detail,
                            f.value:properties:felt::varchar AS felt,
                            f.value:properties:cdi::varchar AS cdi,
                            f.value:properties:mmi::varchar AS mmi,
                            f.value:properties:alert::varchar as alert,
                            f.value:properties:status::varchar as status,
                            f.value:properties:tsunami::number as tsunami,
                            f.value:properties:sig::number as sig,
                            f.value:properties:net::varchar as net,
                            f.value:properties:code::varchar as id,
                            f.value:properties:ids::varchar as ids,
                            f.value:properties:sources::varchar as sources,
                            f.value:properties:types::varchar as types,
                            f.value:properties:nst::int as nst,
                            f.value:properties:dmin::float as dmin,
                            f.value:properties:rms::float as rms,
                            f.value:properties:gap::number as gap,
                            f.value:properties:magType::varchar as magType,
                            f.value:properties:type::varchar as type,
                            f.value:properties:title::varchar as title,
                            f.value:geometry.type::varchar as geo_type,
                            f.value:geometry.coordinates[0]::float as latitude,
                            f.value:geometry.coordinates[1]::float as longitude,
                            f.value:geometry.coordinates[2]::float as altitude,
                            row_number() over(partition by f.value:properties:code, f.value:properties:time order by f.value:properties:code) as rn
                        FROM
                            {{source('public', 'raw_earthquake_history')}},
                            LATERAL FLATTEN(input => PARSE_JSON(json_value):features) AS f) 
                        
                        SELECT * FROM
                            source_data a1
                        WHERE 
                            rn = 1
                        AND NOT EXISTS(
                            SELECT 1 
                            FROM 
                                {{this}} as a2
                            WHERE 
                                a1.id = a2.id
                                AND a1.registered_at = a2.registered_at)

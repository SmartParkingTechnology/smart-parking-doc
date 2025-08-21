 
   -- Combine latitude and longitude into a single column in the 'latitude, longitude' format without N/S/E/W indicators
  CONCAT(
    -- Latitude as a numeric value, preserving sign for N/S
    CAST(CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT_SCALAR(json, '$.metadata.location'), '$.latitude')) AS FLOAT64) AS STRING),
    ', ',
    -- Longitude as a numeric value, preserving sign for E/W
    CAST(CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT_SCALAR(json, '$.metadata.location'), '$.longitude')) AS FLOAT64) AS STRING)
  ) AS location
  
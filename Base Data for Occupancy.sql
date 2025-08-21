WITH base_data AS (
  SELECT 
    DISTINCT SPLIT(orsId, "#")[SAFE_OFFSET(0)] AS org_ID,
    SPLIT(orsId, "#")[SAFE_OFFSET(1)] AS region_ID,
    SPLIT(orsId, "#")[SAFE_OFFSET(2)] AS site_ID,
    inlpreventid,
    COALESCE(inPlate, outplate) AS plate_no,
    DATETIME(TIMESTAMP(arrivalTime), "Europe/London") AS ArrivalDate,
    DATETIME(TIMESTAMP(departureTime), "Europe/London") AS DepartureDate,
    DATETIME(TIMESTAMP(updateTime), "Europe/London") AS UpdateDate,
    -- a continuous date field (based on arrival date)
    DATE(TIMESTAMP(arrivalTime), "Europe/London") AS continuous_date,
    DATETIME_TRUNC(DATETIME(TIMESTAMP(arrivalTime), "Europe/London"), HOUR) AS entry_hour,
    DATETIME(TIMESTAMP_SECONDS(CAST(FLOOR(UNIX_SECONDS(TIMESTAMP(arrivalTime)) / (15 * 60)) * (15 * 60) AS INT64))) AS entry_min,
    DATETIME_TRUNC(DATETIME(TIMESTAMP(departureTime), "Europe/London"), HOUR) AS exit_hour,
    DATETIME(TIMESTAMP_SECONDS(CAST(FLOOR(UNIX_SECONDS(TIMESTAMP(departureTime)) / (15 * 60)) * (15 * 60) AS INT64))) AS exit_min,
    -- Flag for same-day vs next-day exit
    DATE(TIMESTAMP(arrivalTime), "Europe/London") = DATE(TIMESTAMP(departureTime), "Europe/London") AS is_same_day
  FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
  WHERE TIMESTAMP_TRUNC(arrivalTime, DAY) =  TIMESTAMP("2023-01-01") AND action <> 'deleted'-- 
    AND SPLIT(orsId, "#")[SAFE_OFFSET(2)] = "liverpoolMarina"
    AND COALESCE(inPlate, outplate) = "DE71BVF"
),

split_data AS (
  -- First record: Keep entry details, null exit_min if exit is on a different day
  SELECT 
    org_ID,
    region_ID,
    site_ID,
    inlpreventid,
    plate_no,
    continuous_date,
    ArrivalDate,
    DepartureDate,
    UpdateDate,
    entry_hour,
    entry_min,
    CASE 
    WHEN is_same_day THEN exit_hour ELSE NULL END AS exit_hour,
    CASE 
    WHEN is_same_day THEN exit_min ELSE NULL END AS exit_min -- Nullify exit_min if the vehicle exits on a different day
  FROM base_data

  UNION ALL

  -- Generate a new record for the next day if the vehicle exits on the next day
  SELECT 
    org_ID,
    region_ID,
    site_ID,
    inlpreventid,
    plate_no,
    DATE(TIMESTAMP(DepartureDate), "Europe/London") AS continuous_date, -- New continuous date for next day
    --DATETIME(TIMESTAMP(DepartureDate), "Europe/London") AS ArrivalDate, -- Set the new arrival date as the previous departure date
    ArrivalDate,
    DepartureDate,
    UpdateDate,
    NULL AS entry_hour, -- Nullify entry times since it's only tracking exit
    NULL AS entry_min, 
    exit_hour,
    exit_min 
  FROM base_data
  WHERE NOT is_same_day
)

SELECT * FROM split_data
ORDER BY plate_no, continuous_date, ArrivalDate;
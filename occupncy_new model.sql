-- To set a Timezone for organization
WITH timezones AS (
  SELECT DISTINCT *,
         SPLIT(orsId, '#')[SAFE_OFFSET(0)] AS org_id,
         SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS site_id,
         CASE 
           WHEN orsId LIKE '%scmau%' THEN 'Australia/Queensland'
           WHEN orsId LIKE '%scm%' THEN 'Pacific/Auckland'
           WHEN orsId LIKE '%spGermanyManagedService%' THEN 'Europe/Berlin'
           WHEN orsId LIKE '%spukscvs%' THEN 'Europe/London'
           WHEN orsId LIKE '%cityOfMooneeValley%' THEN 'Australia/Victoria'
           WHEN orsId LIKE '%spDenmarkManagedService%' THEN 'Europe/Copenhagen'
         END AS timezone
  FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
  --WHERE DATE(arrivalTime) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
 WHERE  DATE(arrivalTime) between date_sub(parse_date('%Y%m%d',@DS_START_DATE), interval 1 day) and date_add(parse_date('%Y%m%d', @DS_END_DATE), interval 1 day)
),
-- To get siteName, bayCount from group_action
sitegroup AS (
  SELECT DISTINCT groupId, name,
  JSON_VALUE(JSON_EXTRACT(json, '$.metadata.bayCount')) AS bayCount
  FROM `sc-neptune-production.group_actions.group_actions_scm`
  WHERE STRUCT(key, timestamp) IN (
    SELECT STRUCT(key, MAX(timestamp)) 
    FROM `sc-neptune-production.group_actions.group_actions_scm`
    WHERE type = 'site' AND name IN ('New World Orewa', '16 Railside Ave', 'Richmond Centre')
    GROUP BY key
  )
),
filtered_vehicles AS (
  SELECT
    org_id,
    s.groupId as regionId,
    s.name AS sitename,
    InPlate,
    OutPlate,
    datetime(arrivalTime,timezone) as Arrivaldate,
    datetime(departureTime,timezone) as Departuredate,
    updateTime,
    DATETIME_TRUNC(DATETIME(arrivalTime,timezone), HOUR) AS entry_hour,-- to compare with entrytime duration
    DATETIME_TRUNC(DATETIME(departureTime,timezone), HOUR) AS exit_hour,-- to compare with exittime duration
    CASE 
      WHEN arrivalTime IS NOT NULL AND departureTime IS NOT NULL 
      THEN TIMESTAMP_ADD(TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE)), -- Replace 'startTimestamp' with your dynamic field/column
           INTERVAL CAST(FLOOR(DATETIME_DIFF(DATETIME(departureTime, timezone), DATETIME(arrivalTime, timezone), SECOND) / 60) AS INT64) MINUTE)
      ELSE NULL
      END AS StayDurationDateField,
    ROW_NUMBER() OVER (PARTITION BY inlpreventid ORDER BY DATETIME(updateTime, timezone) DESC) AS rn 
  FROM timezones t
  INNER JOIN sitegroup s ON t.site_id = s.groupId
  --WHERE arrivalTime BETWEEN TIMESTAMP('2024-12-01 00:00:00 UTC') AND TIMESTAMP('2024-12-02 23:59:59 UTC')
WHERE DATE(datetime(arrivalTime,timezone)) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)

)
SELECT 
  org_id,
  regionId,
  sitename,
  InPlate,
  outPlate,
  Arrivaldate,
  Departuredate,
    updateTime,
  entry_hour AS hour,
  StayDurationDateField,
  COUNT(DISTINCT CASE 
    WHEN DATETIME_TRUNC(DATETIME(Arrivaldate), HOUR) = entry_hour THEN InPlate 
    ELSE NULL 
  END) AS in_vehicle_count,-- only counting the plate no fall in the perticular entrytime duration
  COUNT(DISTINCT CASE 
    WHEN DATETIME_TRUNC(DATETIME(Departuredate), HOUR) = entry_hour THEN OutPlate 
    ELSE NULL 
  END) AS out_vehicle_count,-- only counting the plate no fall in the perticular exittime duration
  FROM  
  (select distinct * from filtered_vehicles
  where rn = 1)
  GROUP BY org_id, regionId,sitename, entry_hour,Arrivaldate,Departuredate, updateTime,InPlate,OutPlate, StayDurationDateField
ORDER BY sitename, entry_hour;

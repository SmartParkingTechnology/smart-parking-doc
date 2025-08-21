WITH
start_time AS (SELECT PARSE_TIMESTAMP('%Y%m%d %H:%M:%S', CONCAT('20240601', ' 00:00:00')) AS StartTime),  -- Start Date
end_time AS (SELECT PARSE_TIMESTAMP('%Y%m%d %H:%M:%S', CONCAT('20240615', ' 23:59:59')) AS EndTime),   -- End Date
-- Generate all hours in the range for each BayName
baynames AS (
SELECT
  DISTINCT name AS BayName, key
FROM
  `sc-neptune-production.device_actions.device_actions_lesMills`, UNNEST(groupKeys) AS key
WHERE
  STRUCT(reference, timestamp) IN (
  SELECT
    STRUCT(reference, MAX(timestamp))
  FROM
    `sc-neptune-production.device_actions.device_actions_lesMills`
  WHERE
    deviceType = 'sys2'
  GROUP BY
    reference )
  AND actionType != 'DeviceDeleted'
ORDER BY
  name ),
# 1A. Retrieve groups for each bay
baygroups AS (
  select DISTINCT groupId, name as GroupName, type as GroupType, parent as GroupParent
FROM `sc-neptune-production.group_actions.group_actions_lesMills`
where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_lesMills` 
where type !='site' AND type != 'organization' AND type != 'region'
group by key
)
and actionType !='GroupDeleted'
),
# 2. Generate Bay with Hours with no data
all_hours AS (
SELECT
  bn.BayName,bn.key,
  TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y%m%d %H:%M:%S', CONCAT('20240601', ' 00:00:00')), INTERVAL hour_num HOUR) AS DateHour
FROM
  baynames AS bn,
  UNNEST(GENERATE_ARRAY(0, TIMESTAMP_DIFF(PARSE_TIMESTAMP('%Y%m%d %H:%M:%S', CONCAT('20240615', ' 23:59:59')), PARSE_TIMESTAMP('%Y%m%d %H:%M:%S', CONCAT('20240601', ' 00:00:00')), HOUR))) AS hour_num),  
-- 3. Generate hourly intervals with bays and calculate occupied percentage
hourly_intervals AS (
SELECT
  BayName,
  ArrivalTime,
  DepartureTime,
  ARRAY(
  SELECT
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(ArrivalTime, HOUR), INTERVAL i HOUR)
  FROM
    UNNEST(GENERATE_ARRAY(0, TIMESTAMP_DIFF(DepartureTime, TIMESTAMP_TRUNC(ArrivalTime, HOUR), HOUR))) AS i ) AS HourlyRange
FROM (
  SELECT
    DISTINCT deviceName AS BayName,
    DATETIME(startTime,'Pacific/Auckland') AS ArrivalTime,
    DATETIME(currentStateTime,'Pacific/Auckland') AS DepartureTime,
    ROW_NUMBER() OVER (PARTITION BY deviceName, startTime ORDER BY currentStateTime ASC) AS rn
  FROM
    `sc-neptune-production.smartpark.parking_events_v2`
  WHERE
    DATE(currentStateTime,'Pacific/Auckland') BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d', '20240601'), INTERVAL 1 DAY) -- Start Date
    AND DATE_ADD(PARSE_DATE('%Y%m%d', '20240615'), INTERVAL 1 DAY) -- End Date
    AND ORGANIZATION = 'lesMills'
    AND (LOWER(SPLIT(currentState, ':')[SAFE_OFFSET(0)]) = 'parkingsessionend'
      OR LOWER(currentState) = 'vacant') ) where rn =1 ),
-- 4. Flatten the array and calculate occupancy percentage and occupied minutes
flattened_intervals AS (
SELECT
  BayName,
  TIMESTAMP_ADD(TIMESTAMP_TRUNC(ArrivalTime, HOUR), INTERVAL h HOUR) AS Hour,
  -- Include fractional minutes in OccupiedFraction
  TIMESTAMP_DIFF(
    LEAST(DepartureTime, TIMESTAMP_ADD(TIMESTAMP_TRUNC(ArrivalTime, HOUR), INTERVAL h + 1 HOUR)), 
    GREATEST(ArrivalTime, TIMESTAMP_ADD(TIMESTAMP_TRUNC(ArrivalTime, HOUR), INTERVAL h HOUR)),
    SECOND
  )/3600.0 AS OccupiedFraction, 
  TIMESTAMP_DIFF(
    LEAST(DepartureTime, TIMESTAMP_ADD(TIMESTAMP_TRUNC(ArrivalTime, HOUR), INTERVAL h + 1 HOUR)), 
    GREATEST(ArrivalTime, TIMESTAMP_ADD(TIMESTAMP_TRUNC(ArrivalTime, HOUR), INTERVAL h HOUR)),
    SECOND
  )/60.0 AS OccupiedMinutes
FROM
  hourly_intervals, #Table 3
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(HourlyRange) - 1)) AS h
),
-- 5. Aggregate the results to ensure OccupiedPercentage and OccupiedMinutes do not exceed their maximum values
aggregated_intervals AS (
SELECT
  BayName,
  CAST(Hour AS TIMESTAMP) AS DateHour,
  LEAST(SUM(OccupiedFraction), 1) AS OccupiedPercentage,
  SUM(OccupiedMinutes) AS TotalOccupiedMinutes
FROM
  flattened_intervals
GROUP BY
  BayName,
  Hour ),
-- 6. Load online status for each bay, with 3 hours interval + offset hours
BayOnlineStatus3HourInterval AS (
  SELECT
    bayName,
    CASE
      WHEN latestMsgTime IS NOT NULL AND DATETIME_DIFF(DATETIME(TIMESTAMP(viewTimeStamp, 'UTC')), latestMsgTime, MINUTE) <= 180
      THEN 1 ELSE 0
    END AS OnlineHours,
    TIMESTAMP_TRUNC(TIMESTAMP(viewTimeStamp, 'UTC'), HOUR) as DateHour,
    GENERATE_ARRAY(0, 2) AS hour_offsets
  FROM
    `sc-neptune-production.com_smartcloud_smartzone_lesMills.sensor_status`
  WHERE viewTimestamp BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d', '20240601'), INTERVAL 1 DAY) AND DATE_ADD(PARSE_DATE('%Y%m%d', '20240615'), INTERVAL 1 DAY) 
),
-- 7. Transform bay online status to per hour
BayOnlineStatusPerhour As (
SELECT
  bayName,
  OnlineHours as OnlineStatus,
  TIMESTAMP_ADD(DateHour, INTERVAL offset HOUR) AS DateHour
FROM
  BayOnlineStatus3HourInterval,
  UNNEST(hour_offsets) AS offset
),
-- Finally, Join with all hours to ensure every hour is represented, as well as joining the online status
final_result AS (
SELECT
  bg.GroupType,
  split(key, '#')[SAFE_OFFSET(1)] as Key,
  ah.DateHour,
  ah.BayName,
  COALESCE(ai.OccupiedPercentage, 0) AS OccupiedPercentage,
  COALESCE(ai.TotalOccupiedMinutes, 0) AS TotalOccupiedMinutes,
  CASE WHEN os.OnlineStatus is null THEN 0 ELSE os.OnlineStatus END as onlineStatus
FROM
  all_hours AS ah
LEFT JOIN baygroups AS bg ON
   ah.key = CONCAT(bg.GroupParent, '#', bg.groupId)  --HERE
LEFT JOIN aggregated_intervals AS ai ON
  ah.DateHour = ai.DateHour
  AND ah.BayName = ai.BayName 
LEFT JOIN BayOnlineStatusPerhour as os ON
  os.DateHour = ah.DateHour
  AND ah.BayName = os.bayName
)

-- Select the final result
SELECT
  BayName,
  GroupType,
  DateHour,
  OccupiedPercentage,
  TotalOccupiedMinutes,
  onlineStatus
FROM
  final_result
ORDER BY
  BayName,
  DateHour;
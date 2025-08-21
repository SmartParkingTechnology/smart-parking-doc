WITH sc AS (
  SELECT
    'smartcloud' AS dataLocation,
    dim_site.organization,
    fact_visit.entry_time,
    fact_visit.exit_time,
    fact_visit.plate_no,
    fact_visit.stay_duration_in_minute,
    dim_vehicle_indicator.is_whitelisted,
    transitionDate,
    FORMAT_DATE('%A', dim_entry_date.dim_entry_date) AS Day,
    1 AS no_of_visit,
    LAG(entry_time, 1, NULL) OVER (PARTITION BY fact_visit.plate_no, dim_site.site_id ORDER BY fact_visit.entry_time) AS prev_entry_time,
    ROW_NUMBER() OVER (PARTITION BY fact_visit.plate_no ORDER BY entry_time DESC) AS rn,
    EXTRACT(WEEK FROM fact_visit.entry_time) AS current_week,
    EXTRACT(YEAR FROM fact_visit.entry_time) AS current_year,
    EXTRACT(WEEK FROM DATE_SUB(fact_visit.entry_time, INTERVAL 1 WEEK)) AS previous_week,
    EXTRACT(YEAR FROM DATE_SUB(fact_visit.entry_time, INTERVAL 1 WEEK)) AS previous_year
  FROM `sc-neptune-production.managed_services_analytics.fact_visit` fact_visit
  LEFT JOIN `sc-neptune-production.managed_services_analytics.dim_site` dim_site
    ON fact_visit.dim_site_key = dim_site.dim_site_key
  LEFT JOIN `managed_services_analytics.dim_vehicle` dim_vehicle
    ON fact_visit.dim_vehicle_key = dim_vehicle.dim_vehicle_key
  LEFT JOIN `managed_services_analytics.dim_entry_date` dim_entry_date
    ON fact_visit.dim_entry_date_key = dim_entry_date.dim_entry_date_key
  LEFT JOIN `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
    ON fact_visit.dim_vehicle_indicator_key = dim_vehicle_indicator.dim_vehicle_indicator_key
  FULL JOIN `sc-neptune-production.smartcloud.transitionToSmartcloudDate` t
    ON dim_site.ors_id = t.orsId
  WHERE dim_site.organization = @orgId
    AND dim_site.region = @regionId
    AND dim_site.site_id = @siteId
    AND fact_visit.dim_entry_date BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate)
    AND dim_entry_date.dim_entry_date >= transitionDate
),

scEntry AS (
  SELECT *,
    CASE
      WHEN DATE(prev_entry_time) >= DATE_SUB(DATE(entry_time), INTERVAL 30 DAY) THEN MAX(rn) + 1
      ELSE MAX(rn)
    END AS entrycount
  FROM sc
  GROUP BY dataLocation, organization, entry_time, exit_time, plate_no, stay_duration_in_minute, is_whitelisted, transitionDate, Day, no_of_visit, prev_entry_time, rn, current_week, current_year, previous_week, previous_year
),

sr AS (
  WITH matchingPlates AS (
    SELECT DISTINCT *,
      'smartrep' AS dataLocation,
      ROW_NUMBER() OVER (PARTITION BY inlpreventid ORDER BY DATETIME(updateTime, 'Europe/London') DESC) AS rn,
      DATETIME(arrivalTime, 'Europe/London') AS ArrivalDate,
      DATETIME(departuretime, 'Europe/London') AS DepartureDate,
      CASE
        WHEN arrivaltime IS NOT NULL AND departureTime IS NOT NULL THEN ROUND((DATETIME_DIFF(DATETIME(departureTime, 'Europe/London'), DATETIME(arrivalTime, 'Europe/London'), SECOND)) / 60, 1)
        ELSE 0
      END AS StayDurationMinutes
    FROM `sc-neptune-production.smartcloud.lpr_matching_plates_historical`
    WHERE DATE(arrivaltime, 'Europe/London') BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate)
      AND action != 'deleted'
      AND SPLIT(orsId, '#')[SAFE_OFFSET(0)] = @orgId
      AND SPLIT(orsId, '#')[SAFE_OFFSET(1)] = @regionId
      AND SPLIT(orsId, '#')[SAFE_OFFSET(2)] = @siteId
  )
  SELECT dataLocation, matchingPlates.orsId, arrivaldate, departureDate, inplate, stayDurationMinutes, IF(inPlate IS NULL, true, false) AS dummy, transitionDate, FORMAT_DATE('%A', arrivalDate) AS Day, 1 AS no_of_visit,
    LAG(arrivaldate, 1, NULL) OVER (PARTITION BY inplate, matchingPlates.orsId ORDER BY arrivaldate) AS prev_entry_time,
    ROW_NUMBER() OVER (PARTITION BY inplate ORDER BY arrivaltime DESC) AS rn,
    EXTRACT(WEEK FROM arrivaldate) AS current_week,
    EXTRACT(YEAR FROM arrivaldate) AS current_year,
    EXTRACT(WEEK FROM DATE_SUB(arrivaldate, INTERVAL 1 WEEK)) AS previous_week,
    EXTRACT(YEAR FROM DATE_SUB(arrivaldate, INTERVAL 1 WEEK)) AS previous_year
  FROM matchingPlates
  FULL JOIN `sc-neptune-production.smartcloud.transitionToSmartcloudDate` t
    ON matchingPlates.orsid = t.orsid
  WHERE rn = 1
    AND arrivalDate < transitionDate
    AND stayDurationMinutes <= 10000
),

srEntry AS (
  SELECT *,
    CASE
      WHEN DATE(prev_entry_time) >= DATE_SUB(DATE(arrivaldate), INTERVAL 30 DAY) THEN MAX(rn) + 1
      ELSE MAX(rn)
    END AS entrycount
  FROM sr
  GROUP BY dataLocation, orsId, arrivaldate, departuredate, inplate, stayDurationMinutes, dummy, transitionDate, Day, no_of_visit, prev_entry_time, rn, current_week, current_year, previous_week, previous_year
),

scAndSr AS (
  SELECT
    dataLocation,
    organization,
    entry_time,
    exit_time,
    plate_no,
    stay_duration_in_minute,
    is_whitelisted,
    transitionDate,
    Day,
    no_of_visit,
    prev_entry_time,
    rn,
    current_week,
    current_year,
    previous_week,
    previous_year,
    MAX(entrycount) AS entrycount,
    NULL AS dummy -- Add dummy to match the column count
  FROM scEntry
  GROUP BY dataLocation, organization, entry_time, exit_time, plate_no, stay_duration_in_minute, is_whitelisted, transitionDate, Day, no_of_visit, prev_entry_time, rn, current_week, current_year, previous_week, previous_year

  UNION ALL

  SELECT
    dataLocation,
    orsId AS organization, -- Renaming orsId to organization to match the column name
    arrivaldate AS entry_time,
    departureDate AS exit_time,
    inplate AS plate_no,
    stayDurationMinutes AS stay_duration_in_minute,
    NULL AS is_whitelisted, -- Add NULL for is_whitelisted to match the column count
    transitionDate,
    Day,
    no_of_visit,
    prev_entry_time,
    rn,
    current_week,
    current_year,
    previous_week,
    previous_year,
    MAX(entrycount) AS entrycount,
    dummy
  FROM srEntry
  GROUP BY dataLocation, orsId, arrivaldate, departuredate, inplate, stayDurationMinutes, dummy, transitionDate, Day, no_of_visit, prev_entry_time, rn, current_week, current_year, previous_week, previous_year
),

current_and_previous_visits AS (
  SELECT
    organization,
    SUM(CASE WHEN current_week = EXTRACT(WEEK FROM CURRENT_DATE()) AND current_year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN no_of_visit ELSE 0 END) AS current_week_visits,
    SUM(CASE WHEN previous_week = EXTRACT(WEEK FROM CURRENT_DATE() - INTERVAL 1 WEEK) AND previous_year = EXTRACT(YEAR FROM CURRENT_DATE() - INTERVAL 1 WEEK) THEN no_of_visit ELSE 0 END) AS previous_week_visits
  FROM scAndSr
  GROUP BY organization
)

SELECT scAndSr.*, current_and_previous_visits.current_week_visits, current_and_previous_visits.previous_week_visits
FROM scAndSr
JOIN current_and_previous_visits ON scAndSr.organization = current_and_previous_visits.organization
WHERE CASE
  WHEN entry_time < transitionDate THEN dataLocation = 'smartrep'
  WHEN entry_time >= transitionDate THEN dataLocation = 'smartcloud'
END
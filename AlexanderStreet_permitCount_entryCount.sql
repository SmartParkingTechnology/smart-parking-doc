WITH permit_purchase AS (
  SELECT
    CASE
      WHEN siteID IN ('alexanderStreetCarPark1Ballymena', 'alexanderStreetCarPark2Ballymena') THEN 'alexanderStreetCarPark'
      ELSE siteID
    END AS sitename,
    siteID,
    FORMAT_DATE('%Y-%m', DATE(DATETIME(startTime, 'Europe/London'))) AS permit_month,
    DATETIME(startTime, 'Europe/London') AS permit_start,
    CASE
      WHEN amount = 40 THEN DATE_ADD(DATETIME(startTime,'Europe/London'), INTERVAL 30 DAY)
      WHEN amount = 12.5 THEN DATE_ADD(DATETIME(startTime,'Europe/London'), INTERVAL 7 DAY)
      WHEN amount = 90 THEN DATE_ADD(DATETIME(startTime,'Europe/London'), INTERVAL 90 DAY)
      ELSE NULL
    END AS permit_end,
    vehicleRegistration,
    amount
  FROM `sc-neptune-production.smartcloud.Permits_and_Payments_uk`
  WHERE amount IN (12.5, 40, 90)
    AND regionID LIKE '%alexanderProperty%' 
    AND DATE(startTime) BETWEEN DATE('2025-01-01') AND DATE('2025-12-31')
),
visits AS (
  SELECT DISTINCT
    CASE
      WHEN SPLIT(orsId, '#')[SAFE_OFFSET(2)] IN ('alexanderStreetCarPark1Ballymena', 'alexanderStreetCarPark2Ballymena') THEN 'alexanderStreetCarPark'
      ELSE SPLIT(orsId, '#')[SAFE_OFFSET(2)]
    END AS sitename,
    DATETIME(arrivalTime, 'Europe/London') AS arrivalTime,
    DATETIME(departureTime, 'Europe/London') AS departureTime,
    inPlate
  FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
  WHERE TIMESTAMP_TRUNC(arrivalTime, DAY) >= TIMESTAMP("2025-01-01") 
    AND orsId LIKE '%alexanderStreet%'
)
SELECT
  p.permit_month,
  p.sitename,
  p.vehicleRegistration,
  p.amount,
  COUNT(v.inPlate) AS entry_count
FROM permit_purchase p
LEFT JOIN visits v
  ON p.sitename = v.sitename
  AND p.vehicleRegistration = v.inPlate
  AND (
    v.arrivalTime BETWEEN p.permit_start AND p.permit_end OR
    v.departureTime BETWEEN p.permit_start AND p.permit_end
  )
GROUP BY p.permit_month, p.sitename, p.vehicleRegistration, p.amount
ORDER BY p.permit_month DESC, p.sitename, p.vehicleRegistration;
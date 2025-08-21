SELECT
      CASE
      WHEN siteID IN ('alexanderStreetCarPark1Ballymena', 'alexanderStreetCarPark2Ballymena') THEN 'alexanderStreetCarPark'
      ELSE siteID
    END AS sitename,
    vehicleRegistration,
    amount,
   (DATETIME(startTime, 'Europe/London')) AS permit_start,
    CASE
      WHEN amount = 40 THEN DATE_ADD((DATETIME(startTime,'Europe/London')), INTERVAL 29 DAY)
      WHEN amount = 12.5 THEN DATE_ADD((DATETIME(startTime,'Europe/London')), INTERVAL 6 DAY)
      WHEN amount = 90 THEN DATE_ADD((DATETIME(startTime,'Europe/London')), INTERVAL 89 DAY)
      ELSE NULL
    END AS permit_end
  FROM `sc-neptune-production.smartcloud.Permits_and_Payments_uk`
  WHERE amount IN (12.5, 40, 90)
    AND regionID LIKE '%alexanderProperty%' 
    --AND vehicleRegistration = 'CC61RYL'
    AND DATE(startTime) BETWEEN DATE('2025-01-01') AND DATE('2025-12-31')
),
purchase_count as(
SELECT
  sitename,
  FORMAT_DATE('%Y-%m', permit_start) AS permit_month,
  permit_start,
  permit_end,
  vehicleRegistration,
  amount,
  COUNT(*) AS purchase_count
FROM (
  -- Deduplicate: Only one purchase per sitename, vehicle, amount, and permit_start (across both sites)
  SELECT DISTINCT
    sitename,
    permit_start,
    permit_end,
    vehicleRegistration,
    amount
  FROM purchases
)
GROUP BY sitename, permit_month,permit_start, permit_end, vehicleRegistration, amount
ORDER BY sitename, permit_month, vehicleRegistration, amount
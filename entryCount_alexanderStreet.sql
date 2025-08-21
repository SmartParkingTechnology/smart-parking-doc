WITH purchases AS (
  SELECT 
    sitename,
    --siteID,
    vehicleRegistration,
    amount,
    FORMAT_DATE('%Y-%m', DATE(permit_start)) AS permit_month,
    permit_start,
    permit_end,
    COUNT(*) AS purchase_count,
     FROM(
SELECT
siteID, 
  CASE
      WHEN siteID IN ('alexanderStreetCarPark1Ballymena', 'alexanderStreetCarPark2Ballymena') THEN 'alexanderStreetCarPark'
      ELSE siteID
    END AS sitename,
  vehicleRegistration,
  amount,
  DATETIME(startTime, 'Europe/London') AS permit_start,
  CASE
    WHEN amount = 40 THEN DATE_ADD(DATETIME(startTime,'Europe/London'), INTERVAL 30 DAY)
    WHEN amount = 12.5 THEN DATE_ADD(DATETIME(startTime,'Europe/London'), INTERVAL 7 DAY)
    WHEN amount = 90 THEN DATE_ADD(DATETIME(startTime,'Europe/London'), INTERVAL 90 DAY)
    ELSE NULL
  END AS permit_end,
  row_number() over (partition by startTime order by amount) as rn
  FROM `sc-neptune-production.smartcloud.Permits_and_Payments_uk`
WHERE amount IN (12.5, 40, 90)
  AND regionID LIKE '%alexanderProperty%' 
  --AND vehicleRegistration = 'CC61RYL'
  AND DATE(startTime) BETWEEN DATE('2025-02-01') AND DATE('2025-12-31')
ORDER BY siteID, startTime
 )
where rn =1
GROUP BY sitename, permit_start, permit_end, permit_month, vehicleRegistration, amount
order by vehicleRegistration, amount,permit_start
),
visits AS (
--To get all the visit/in/arrival data for a plate for each site
  SELECT DISTINCT
    inPlate,
    CASE
      WHEN SPLIT(orsId, '#')[SAFE_OFFSET(2)] = 'alexanderStreetCarPark1Ballymena' THEN 'alexanderStreetCarPark'
      WHEN SPLIT(orsId, '#')[SAFE_OFFSET(2)] = 'alexanderStreetCarPark2Ballymena' THEN 'alexanderStreetCarPark'
    END AS sitename,
    DATETIME(arrivalTime, 'Europe/London') AS arrivalTime,
    DATETIME(departureTime, 'Europe/London') AS departureTime,
    
  FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
  WHERE TIMESTAMP_TRUNC(arrivalTime, DAY) >= TIMESTAMP("2025-02-01") 
    AND orsId LIKE '%alexanderStreet%'
    --AND inPlate = 'CC61RYL'
    ORDER BY arrivalTime
),
joined AS (
  SELECT 
    p.vehicleRegistration,
    p.sitename,
    p.permit_month,
    p.amount,
    COUNT( v.arrivalTime) AS entry_count -- sitewise count of plate for each month
  FROM purchases p
  LEFT JOIN visits v
    ON p.sitename = v.sitename
    AND p.vehicleRegistration = v.inPlate
    AND (
      v.arrivalTime BETWEEN p.permit_start AND p.permit_end OR
      v.departureTime BETWEEN p.permit_start AND p.permit_end
    ) -- to make sure we will get the count only for a perticular 'startTime' and 'endTime' from permits CTE
  GROUP BY p.vehicleRegistration, p.sitename,  p.permit_month, p.amount
)

SELECT 
  j.permit_month,
  j.sitename,
  j.vehicleRegistration,
  j.amount,
  j.entry_count,
  COUNT(pc.purchase_count) AS purchase_count
FROM joined j
LEFT JOIN purchases pc
  ON j.vehicleRegistration = pc.vehicleRegistration
  AND j.permit_month = pc.permit_month
  GROUP BY j.vehicleRegistration, j.permit_month,j.sitename,j.amount, j.entry_count
ORDER BY j.permit_month DESC;
WITH vehicle_counts AS (
  SELECT 
    FORMAT_DATE('%Y-%m', DATE(DATETIME(startTime, 'Europe/London'))) AS permit_month,
    amount,
    COUNT(DISTINCT vehicleRegistration) AS vehicle_count
  FROM `sc-neptune-production.smartcloud.Permits_and_Payments_uk`
  WHERE regionID LIKE '%alexanderProperty%'
  GROUP BY permit_month, amount
  ORDER BY permit_month, amount
)
SELECT * FROM vehicle_counts 

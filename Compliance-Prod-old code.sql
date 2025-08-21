WITH permits AS 
(SELECT transactionId, orgId, regionId, siteId, permitSource AS Source, vehicleRegistration, startTime, endTime, permitId, permitType, CAST(NULL AS float64) AS duration, amount,
ROW_NUMBER() OVER (PARTITION BY regionId, siteId, vehicleRegistration, transactionID) AS rn,
CASE WHEN orgId = 'scmau' THEN 'Australia/Queensland' 
WHEN orgId = 'scm' THEN 'Pacific/Auckland' 
WHEN orgId = 'spukscvs' THEN 'Europe/London' 
WHEN orgId = 'spGermanyManagedService' THEN 'Europe/Berlin'
WHEN orgId = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' WHEN orgId = 'cityOfMooneeValley' THEN 'Australia/Victoria' END AS timezone  
FROM `sc-neptune-production.permit.permit_transactions` WHERE orgId = @orgId AND regionId = @regionId AND siteId = @siteId AND DATE(startTime) BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d',@startDate), INTERVAL 1 DAY) AND DATE_ADD(PARSE_DATE('%Y%m%d', @endDate), INTERVAL 1 day) 
  AND NOT starts_with(permitSource, CONCAT(@orgId, '#', @regionId, '#', @siteId, '#')) # this line to ensure exemption camera will not show up
),

payment AS (
SELECT SPLIT(orsId, '#')[SAFE_OFFSET(0)] AS orgId, SPLIT(orsId, '#')[SAFE_OFFSET(1)] AS regionId, SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS siteId, SPLIT(paymentSource, '#')[SAFE_OFFSET(0)] AS Source, vehicleRegistration, startTime, endTime,totalamount, amount, (durationSeconds/60) AS duration, transactionStatus,transactionID, 
CASE WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'scmau' THEN 'Australia/Queensland' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'scm' THEN 'Pacific/Auckland' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spukscvs' THEN 'Europe/London' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spGermanyManagedService' THEN 'Europe/Berlin' 
WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'cityOfMooneeValley' THEN 'Australia/Victoria' END AS timezone,
ROW_NUMBER() OVER (PARTITION BY SPLIT(orsId, '#')[SAFE_OFFSET(1)], SPLIT(orsId, '#')[SAFE_OFFSET(2)], vehicleRegistration ORDER BY transactionRequestTime desc) AS rn 
FROM `sc-neptune-production.smartcloud.payment_transactions` 
WHERE SPLIT(orsId, '#')[SAFE_OFFSET(0)] = @orgId AND SPLIT(orsId, '#')[SAFE_OFFSET(1)] = @regionId AND SPLIT(orsId, '#')[SAFE_OFFSET(2)] = @siteId AND date(startTime) BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d',@startDate), INTERVAL 1 DAY) AND DATE_ADD(PARSE_DATE('%Y%m%d',@endDate), INTERVAL 1 DAY) 
) 

SELECT totalamount,amount, @startDate AS startDateParam, @endDate AS endDateParam, orgId, regionId, siteId, Source,transactionId,vehicleRegistration, DATETIME(startTime, timezone) AS StartTime, DATETIME(endTime, timezone) AS EndTime, 'Payment' AS Type, duration FROM payment
WHERE DATE(startTime, timezone) >= date_sub(current_date(timezone), interval 7 MONTH) and DATE(startTime, timezone) BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d',@endDate) AND rn = 1
UNION ALL
SELECT null AS totalamount ,amount, @startDate AS startDateParam,@endDate AS endDateParam, orgId, regionId, siteId, Source,transactionId, vehicleRegistration, DATETIME(startTime, timezone) AS startTimeLocal, DATETIME(endTime, timezone) AS endTimeLocal, 'Permit' AS Type, duration FROM permits 
WHERE DATE(startTime, timezone) >= DATE_SUB(current_date(timezone), interval 7 MONTH) AND DATE(startTime, timezone) BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate) AND rn = 1
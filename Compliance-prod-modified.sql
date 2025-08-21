WITH permits AS(
  
   SELECT  orgID AS orgId, regionID AS regionId, siteID AS siteId, permitSource AS Source, vehicleRegistration, startTime,              endTime,transactionRequestTime, permitID, permitType, CAST(NULL AS float64) AS duration, amount,transactionID,
           ROW_NUMBER() OVER (PARTITION BY regionID, siteID, vehicleRegistration, transactionID) AS rn,
           CASE 
            WHEN orgID = 'scmau' THEN 'Australia/Queensland' 
            WHEN orgID = 'scm' THEN 'Pacific/Auckland' 
            WHEN orgID = 'spukscvs' THEN 'Europe/London' 
            WHEN orgID = 'spGermanyManagedService' THEN 'Europe/Berlin'
            WHEN orgID = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' 
            WHEN orgID = 'cityOfMooneeValley' THEN 'Australia/Victoria' END AS timezone  
   FROM `sc-neptune-production.permit.permit_transactions` 
         WHERE orgID = @orgId AND regionID = @regionId AND siteID = @siteId AND 
         DATE(startTime) BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d',@startDate), INTERVAL 1 DAY) AND     
         DATE_ADD(PARSE_DATE('%Y%m%d', @endDate), INTERVAL 1 day) AND 
         NOT starts_with(permitSource, CONCAT(@orgId, '#', @regionId, '#', @siteId, '#')) -- this line to ensure exemption camera will not show up
             ),

AggregatedData AS (
  SELECT * from (
    SELECT  
        MIN(startTime) AS startTime, MAX(endTime) AS endTime, SPLIT(orsId, '#')[SAFE_OFFSET(0)] AS orgId,
        SPLIT(orsId, '#')[SAFE_OFFSET(1)] AS regionId, SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS siteId,
        CONCAT(groupParent, '#', groupId) AS groupReference, userId, userEmail, firstName, lastName,    
        vehicleRegistration, MAX( amount) AS amount,MAX(totalAmount)as TotalAmount, transactionRequestTime,   
        transactionResponseTime, transactionID, transactionStatus, '' AS permitID, '' AS reason, paymentSource AS Source,deviceName, SUM(durationSeconds / 60) AS duration,
        CASE 
           WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'scmau' THEN 'Australia/Queensland' 
           WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'scm' THEN 'Pacific/Auckland' 
           WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spukscvs' THEN 'Europe/London' 
           WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spGermanyManagedService' THEN 'Europe/Berlin' 
           WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' 
           WHEN SPLIT(orsId, '#')[SAFE_OFFSET(0)] = 'cityOfMooneeValley' THEN 'Australia/Victoria' END AS timezone,
    FROM `sc-neptune-production.smartcloud.payment_transactions` 
        WHERE SPLIT(orsId, '#')[SAFE_OFFSET(0)] = @orgId AND SPLIT(orsId, '#')[SAFE_OFFSET(1)] = @regionId AND
        SPLIT(orsId, '#')[SAFE_OFFSET(2)] = @siteId  AND 
        DATE(startTime) BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d',@startDate), INTERVAL 1 YEAR) AND     
        DATE_ADD(PARSE_DATE('%Y%m%d', @endDate), INTERVAL 1 YEAR) # load all payment trasnactions, some of them are break down by 7 days and continuously lasts for 1 year or 1 month
        --DATE(start Time) >= DATE('2011-01-01')
        GROUP BY 
           vehicleRegistration, userId, userEmail, firstName, lastName,transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, paymentSource, deviceName, groupId,groupParent, orsId
  ) where DATE(startTime) BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d', @endDate) --this is to ensure only the payment made on the 'startTime' is between the selection of the date range, otherwise it won't show
),
LatestRecord AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY vehicleRegistration, startTime, endTime, Source ORDER BY transactionRequestTime DESC) AS rn
    FROM AggregatedData
)

--Combine with other data sources using UNION ALL
SELECT 
    startTime,
    PARSE_DATE('%Y%m%d',@startDate) AS reportStartDay,
    endTime,
    PARSE_DATE('%Y%m%d',@endDate) AS reportEndDay,
    transactionRequestTime,
    regionId, 
    siteId, 
    vehicleRegistration, 
    amount,
    TotalAmount,
    transactionID, 
    permitID, 
    Source, 
    duration,
    Type,
rn
   
FROM (
    
    SELECT  startTime, endTime, @startDate, @endDate, transactionRequestTime, orgId,regionId, siteId, vehicleRegistration, amount, null as TotalAmount, transactionID, 
            permitID, Source, CAST(NULL AS float64) AS duration, rn, 'Permit' AS Type  
    FROM permits 
         WHERE rn = 1 
         --and DATE(startTime, timezone) >= date_sub(current_date(timezone), interval 7 month) AND DATE(startTime, timezone)          BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate)
       --where orgId = 'spDenmarkManagedService' AND DATE(startTime) >= DATE('2011-01-01')
    UNION ALL

    SELECT  startTime, endTime,@startDate, @endDate, transactionRequestTime, orgId, regionId, siteId, vehicleRegistration, amount, TotalAmount, transactionID, 
            permitID, Source, duration, rn, 'payment' AS Type,
    FROM LatestRecord
    WHERE rn = 1 
    --and DATE(startTime, timezone) >= date_sub(current_date(timezone), interval 7 month) and DATE(startTime, timezone)     
    --BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d',@endDate)
    --and vehicleRegistration = 'CA20334'
    --and TIMESTAMP_TRUNC(startTime, DAY) >= TIMESTAMP("2024-08-26")
) AS CombinedData

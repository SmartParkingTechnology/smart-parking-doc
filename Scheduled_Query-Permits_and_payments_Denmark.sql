create or replace table `sc-neptune-production.smartcloud.Permits_and_Payments_denmark`
(
  startTime timestamp,
  endTime timestamp,
  regionID string,
  siteID string,
  groupReference string,
 userId string,
 userEmail string,
 firstName string,
  lastName string,
  vehicleRegistration string,
  amount float64,
  transactionRequestTime timestamp,
  transactionId string,
  transactionStatus string,
  permitID string,
  source string,
  reason string,
  deviceName string,
  duration,
  rowNum
)
cluster by 
regionID,
siteID
OPTIONS (
    description = 'a table clustered by regionID,siteID');


--insert data into Cluster Table
insert into `sc-neptune-production.smartcloud.Permits_and_Payments_denmark`
(
  -- Define CTEs
WITH AggregatedData AS (
    SELECT  
        MIN(startTime) AS startTime,MAX(endTime) AS endTime, SPLIT(orsId, '#')[SAFE_OFFSET(1)] AS regionId, SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS siteId,CONCAT(groupParent, '#', groupId) AS groupReference,
        userId, userEmail, firstName,lastName, vehicleRegistration,SUM(amount) AS amount, transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, '' AS permitID, '' AS reason, 
        paymentSource AS source,  deviceName, SUM(durationSeconds / 60) AS duration
    FROM `sc-neptune-production.smartcloud.payment_transactions` 
    WHERE STARTS_WITH(orsId, 'spDenmarkManagedService#') 
      AND DATE(startTime) >= DATE('2011-01-01')
    GROUP BY 
        vehicleRegistration, userId, userEmail, firstName, lastName,transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, paymentSource, deviceName, groupId,groupParent, orsId
),
LatestRecord AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY vehicleRegistration, startTime, endTime ORDER BY transactionRequestTime DESC) AS rowNum
    FROM AggregatedData
)

-- Combine with other data sources using UNION ALL
SELECT 
    startTime, 
    endTime, 
    regionId, 
    siteId, 
    groupReference, 
    userId, 
    userEmail, 
    firstName, 
    lastName,
    vehicleRegistration, 
    amount, 
    transactionRequestTime,
    transactionResponseTime, 
    transactionID, 
    transactionStatus, 
    permitID, 
    reason, 
    source, 
    deviceName,
    duration,
    rowNum
FROM (
    -- First data source
    SELECT  startTime, endTime, regionId, siteId, groupReference, userId, userEmail, firstName, lastName,vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, 
            transactionStatus, permitID, reason, permitSource as source, deviceName,CAST(NULL AS float64) AS duration, CAST(NULL AS float64) as rowNum   
    FROM `sc-neptune-production.permit.permit_exemptions_spDenmarkManagedService` 

    UNION ALL

    -- Second data source
    SELECT  startTime, endTime, regionId, siteId, groupReference, userId, userEmail, firstName, lastName,vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, 
            transactionStatus, permitID, reason, permitSource as source, deviceName, CAST(NULL AS float64) AS duration, CAST(NULL AS float64) as rowNum  
    FROM `sc-neptune-production.permit.permit_transactions` 
       where orgId = 'spDenmarkManagedService' AND DATE(startTime) >= DATE('2011-01-01')
    UNION ALL

    -- Third data source from CTE
    SELECT  startTime, endTime, regionId, siteId, groupReference, userId, userEmail, firstName, lastName, vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, 
            transactionStatus,permitID, reason, source, deviceName,duration, rowNum
    FROM LatestRecord
    WHERE rowNum = 1
    
) AS CombinedData
)

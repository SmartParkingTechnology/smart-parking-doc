CREATE OR REPLACE TABLE `sc-neptune-production.smartcloud.Permits_and_Payments_Germany`
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
  transactionResponseTime timestamp,
  transactionId string,
  transactionStatus string,
  permitID string,
  reason string,
  source string,
  deviceName string,
  duration float64,
  rowNum float64
)
CLUSTER BY 
regionID,
siteID
OPTIONS (
    description = 'a table clustered by regionID,siteID');


--insert data into Cluster Table
INSERT INTO `sc-neptune-production.smartcloud.Permits_and_Payments_Germany`
(
  SELECT  startTime, endTime, regionId, siteId, groupReference, userId, userEmail, firstName, lastName,
          vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, permitID,reason, permitSource AS source, deviceName,
          CAST(NULL AS float64) AS duration, CAST(NULL AS FLOAT64) AS rowNum 
  FROM `sc-neptune-production.permit.permit_exemptions_spGermanyManagedService*` 

UNION ALL

  SELECT  startTime, endTime, regionId, siteId, groupReference, userId, userEmail, firstName, lastName,
          vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, permitID,reason, permitSource AS source, deviceName,
          CAST(NULL AS float64) AS duration, CAST(NULL AS FLOAT64) AS rowNum

  FROM `sc-neptune-production.permit.permit_transactions` 
         WHERE orgId = 'spGermanyManagedService' AND DATE(startTime) >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), INTERVAL 7 DAY)

UNION ALL

SELECT * FROM(
   SELECT  startTime, endTime, split(orsId, '#')[SAFE_OFFSET(1)] AS regionId, split(orsId,'#')[SAFE_OFFSET(2)] AS siteId, 
           CONCAT(groupParent, '#', groupId) AS groupReference,userId,userEmail, firstName, lastName,
	   vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, transactionStatus,'' AS permitID, '' AS reason, paymentSource AS source, deviceName,
           (durationSeconds/60) as duration,
           ROW_NUMBER() OVER (PARTITION BY vehicleRegistration, startTime, endTime ORDER BY  transactionRequestTime) AS rowNum
   FROM `sc-neptune-production.smartcloud.payment_transactions` 
           WHERE starts_with(orsId, 'spGermanyManagedService#') AND DATE(startTime) >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), INTERVAL 7 DAY)
          )

   WHERE rowNum = 1
)
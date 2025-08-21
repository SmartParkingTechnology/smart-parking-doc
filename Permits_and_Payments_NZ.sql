CREATE OR REPLACE TABLE `sc-neptune-production.smartcloud.NZ_All_Permits_and_Payments`
(
  organization string,
  regionID string,
  siteID string,
  startTime timestamp,
  endTime timestamp,
  groupReference string,
  userId string,
  userEmail string,
  firstName string,
  lastName string,
  amount float64,
  transactionRequestTime timestamp,
  transactionResponseTime timestamp,
  transactionId string,
  transactionStatus string,
  permitID string,
  vehicleRegistration string,
  reason string,
  source string,
  --recurrenceLocalStartTime timestamp,
  --recurrenceLocalEndTime timestamp,
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
INSERT INTO `sc-neptune-production.smartcloud.NZ_All_Permits_and_Payments`
(
  SELECT orgID AS organization,regionId, siteId, startTime, endTime, groupReference, userId, userEmail, firstName, lastName, amount, transactionRequestTime,transactionResponseTime, transactionID, 
         transactionStatus, permitID,vehicleRegistration, reason, permitSource AS source, deviceName,CAST(NULL AS float64) AS duration, CAST(NULL AS float64) AS rowNum
    FROM `sc-neptune-production.permit.permit_exemptions_scm*` 
WHERE orgID= 'scm'

UNION ALL

SELECT orgId AS organization,regionId, siteId, startTime, endTime,  groupReference, userId, userEmail, firstName, lastName, amount, transactionRequestTime,transactionResponseTime, transactionID, 
      transactionStatus, permitID,vehicleRegistration, reason, permitSource AS source, deviceName,CAST(NULL AS float64) AS duration, CAST(NULL AS float64) AS rowNum
 FROM `sc-neptune-production.permit.permit_transactions` 
WHERE orgId = 'scm' AND DATE(startTime) >= DATE('2011-01-01')

UNION ALL

SELECT * FROM(
     SELECT orsId AS organization,split(orsId, '#')[SAFE_OFFSET(1)] AS regionId, split(orsId,'#')[SAFE_OFFSET(2)] AS siteId, startTime, endTime,  CONCAT(groupParent, '#', groupId) AS groupReference,  
            userId, userEmail, firstName, lastName, amount, transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, '' AS permitID,vehicleRegistration, '' AS reason, paymentSource 
            AS source, deviceName,(durationSeconds/60) AS duration,
            ROW_NUMBER() OVER (PARTITION BY vehicleRegistration, startTime, endTime ORDER BY transactionRequestTime) AS rowNum
             FROM `sc-neptune-production.smartcloud.payment_transactions` WHERE starts_with(orsId, 'scm#') AND DATE(startTime) >= DATE('2011-01-01')
            )
     WHERE rowNum = 1

)
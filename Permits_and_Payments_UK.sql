CREATE OR REPLACE TABLE `sc-neptune-production.smartcloud.Permits_and_Payments_uk`
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
OPTIONS (description = 'a table clustered by regionID,siteID');


--insert data into Cluster Table
INSERT INTO `sc-neptune-production.smartcloud.Permits_and_Payments_uk`
(

  SELECT  startTime, --endTime,
          COALESCE(endTime,TIMESTAMP'2099-01-01 00:00:00 UTC'),--added by Kin
		  regionId, siteId, groupReference, userId, userEmail, firstName, lastName,vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, 
          transactionStatus, permitID, reason, permitSource AS source, deviceName,CAST(NULL AS float64) AS duration,CAST(NULL AS float64) AS rowNum , 
		  CONCAT(regionId,siteId) AS groupKey --added by Kin
        FROM `sc-neptune-production.permit.permit_exemptions_spukscvs*` 

UNION ALL

SELECT  startTime, endTime, regionId, siteId, groupReference, userId, userEmail, firstName, lastName,vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, 
        transactionStatus, permitID, reason,permitSource AS source, deviceName,CAST(NULL AS float64) AS duration,CAST(NULL AS float64) AS rowNum, 
		CONCAT(regionId,siteId) AS groupKey -- added by Kin 
       FROM `sc-neptune-production.permit.permit_transactions` 
    WHERE orgId = 'spukscvs' AND DATE(startTime) >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), INTERVAL 7 DAY)
	UNION ALL

SELECT * FROM (
      SELECT  startTime, endTime, split(orsId, '#')[SAFE_OFFSET(1)] as regionId,split(orsId,'#')[SAFE_OFFSET(2)] as siteId, CONCAT(groupParent, '#', groupId) as groupReference, 
              userId, userEmail, firstName, lastName,vehicleRegistration, amount, transactionRequestTime,transactionResponseTime, transactionID, transactionStatus, '' as permitID, '' as reason,
              paymentSource as source, deviceName, (durationSeconds/60) as duration,
              ROW_NUMBER() OVER (partition by vehicleRegistration, startTime, endTime order by transactionRequestTime) as rowNum,
			  concat(split(orsId, '#')[SAFE_OFFSET(1)],split(orsId,'#')[SAFE_OFFSET(2)]) as groupKey -- added by Kin
 FROM `sc-neptune-production.smartcloud.payment_transactions` 
 WHERE starts_with(orsId, 'spukscvs#') AND DATE(startTime) >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), INTERVAL 7 DAY)
) 
   WHERE rowNum = 1

)


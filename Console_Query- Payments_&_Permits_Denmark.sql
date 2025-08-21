WITH siteGroups AS (
SELECT parent, groupId, name
FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
WHERE struct(key, timestamp) IN
(
SELECT struct(key, MAX(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
WHERE type = 'site'
GROUP BY key
)
AND actionType != 'GroupDeleted'
)
SELECT * EXCEPT (startTime, endTime, transactionRequestTime),
DATETIME(transactionRequestTime, 'Europe/Copenhagen') AS transactionRequestTime,
DATETIME(startTime, 'Europe/Copenhagen') AS startTime,
DATETIME(endTime, 'Europe/Copenhagen') AS endTime,
FROM `sc-neptune-production.smartcloud.Permits_and_Payments_denmark` AS d 
INNER JOIN siteGroups
ON d.regionId = siteGroups.parent AND  d.siteId = siteGroups.groupId
WHERE --DATE(startTime,'Europe/London') BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d',@DS_START_DATE), INTERVAL 32 DAY) AND PARSE_DATE('%Y%m%d',@DS_END_DATE) AND 
(PARSE_DATE('%Y%m%d',@DS_START_DATE) <= DATE(endTime, 'Europe/Copenhagen') AND PARSE_DATE('%Y%m%d',@DS_END_DATE) >= DATE(startTime, 'Europe/Copenhagen'))

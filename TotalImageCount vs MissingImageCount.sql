with devices as (
select 
name as cameraName, 
reference, 
g
FROM `sc-neptune-production.device_actions.device_actions_spukscvs`, unnest(groupKeys) g
where struct(reference, timestamp) IN
(
   select 
      struct(reference, max(timestamp))
   FROM `sc-neptune-production.device_actions.device_actions_spukscvs`
   where deviceType = 'mav-camera'
   group by reference
)
AND actionType != 'DeviceDeleted'),

sites as (
select 
  parent, 
  groupId, 
  name as GroupName
FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) 
FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
where type = 'site'
group by key
)
AND actionType != 'GroupDeleted'
),
 
TotalImages as (
  SELECT
     deviceId, 
	 DATE(fact.timestamp, 'Europe/London') as day,
     SAFE_CAST(CONCAT('0x', REPLACE(JSON_EXTRACT(sourceData, '$.SaFID'),"\"","")) AS INT64) as SaFID,
     SAFE_CAST(CONCAT('0x', REPLACE(JSON_EXTRACT(sourceData, '$.SaFID'),"\"","")) AS INT64) - # this SaFID minus
     LAG(SAFE_CAST(CONCAT('0x', REPLACE(JSON_EXTRACT(sourceData, '$.SaFID'),"\"","")) AS INT64)) OVER (ORDER BY deviceId, SAFE_CAST(CONCAT('0x', REPLACE(JSON_EXTRACT(sourceData, '$.SaFID'),"\"","")) AS INT64) DESC) AS difference # last SaFID
 FROM `sc-neptune-production.smartcloud.lpr_events_v2` 
 WHERE DATE(timestamp, 'Europe/London') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)  and organization = 'spukscvs'

)
select 
  cameraName, 
  GroupName, 
  SaFID, 
  day, 
  difference, 
  deviceId 
from devices 
inner join sites on devices.g = CONCAT(sites.parent, '#', sites.groupId)
inner join TotalImages on devices.reference = CONCAT('mav-camera#', TotalImages.deviceId)
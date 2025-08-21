CREATE TABLE `sc-neptune-production.Dataform_Test.fact_visit`
CLUSTER BY region AS
WITH site_all AS 
   (
  -- Union all site-specific data
    SELECT organization,parent, groupId,GroupName, timezone
    FROM (
        
        SELECT organization,parent, groupId, name AS GroupName, timezone
        FROM `sc-neptune-production.group_actions.group_actions_scm`
        WHERE STRUCT(key, timestamp) IN 
        (
            SELECT STRUCT(key, MAX(timestamp))
            FROM `sc-neptune-production.group_actions.group_actions_scm`
            WHERE type = 'site'
            GROUP BY key
        )
        AND actionType != 'GroupDeleted'
        
        UNION ALL
        
        SELECT organization,parent, groupId, name AS GroupName, timezone
        FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
        WHERE STRUCT(key, timestamp) IN 
        (
            SELECT STRUCT(key, MAX(timestamp))
            FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
            WHERE type = 'site'
            GROUP BY key
        )
        AND actionType != 'GroupDeleted'
        
        UNION ALL
        
        SELECT organization,parent, groupId, name AS GroupName, timezone
        FROM `sc-neptune-production.group_actions.group_actions_scmau`
        WHERE STRUCT(key, timestamp) IN 
        (
            SELECT STRUCT(key, MAX(timestamp))
            FROM `sc-neptune-production.group_actions.group_actions_scmau`
            WHERE type = 'site'
            GROUP BY key
        )
        AND actionType != 'GroupDeleted'
        
        UNION ALL
        
        SELECT organization,parent, groupId, name AS GroupName, timezone
        FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
        WHERE STRUCT(key, timestamp) IN 
        (
            SELECT STRUCT(key, MAX(timestamp))
            FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
            WHERE type = 'site'
            GROUP BY key
        )
        AND actionType != 'GroupDeleted'
        
        UNION ALL
        
        SELECT organization,parent, groupId, name AS GroupName, timezone
        FROM `sc-neptune-production.group_actions.group_actions_spGermanyManagedService`
        WHERE STRUCT(key, timestamp) IN 
        (
            SELECT STRUCT(key, MAX(timestamp))
            FROM `sc-neptune-production.group_actions.group_actions_spGermanyManagedService`
            WHERE type = 'site'
            GROUP BY key
        )
        AND actionType != 'GroupDeleted'
    )
),
-- merging all exempted vehicle data
permit_all as 
   (
         SELECT
         sha256(concat(orgID,regionID,siteID,vehicleregistration)) as permit_key,
         orgID as organization,regionID as region,siteID as site_id,vehicleRegistration as plate_no,transactionid as transaction_id,
         case 
         when timestamp_trunc (starttime,SECOND) < timestamp_trunc(transactionresponsetime,SECOND) 
         then timestamp_trunc(transactionresponsetime,SECOND) 
         else timestamp_trunc (starttime,SECOND) end as start_time_utc,
         case 
         when format_timestamp('%Y',endtime) = '2099'
         then parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC') 
         else timestamp_trunc (ifnull(endtime,parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC')),SECOND) end as end_time_utc,
         timestamp_trunc(transactionrequesttime,SECOND) as transaction_request_time_utc,
         timestamp_trunc(transactionresponsetime,SECOND) as transaction_response_time_utc,
         transactionstatus as transaction_status
         from  `sc-neptune-production.permit.permit_exemptions_scm*`
                         
        UNION ALL

         SELECT
         sha256(concat(orgID,regionID,siteID,vehicleregistration)) as permit_key,
         orgID as organization,regionID as region,siteID as site_id,vehicleRegistration as plate_no,transactionid as transaction_id,
         case 
         when timestamp_trunc (starttime,SECOND) < timestamp_trunc(transactionresponsetime,SECOND) 
         then timestamp_trunc(transactionresponsetime,SECOND) 
         else timestamp_trunc (starttime,SECOND) end as start_time_utc,
         case 
         when format_timestamp('%Y',endtime) = '2099'
         then parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC') 
         else timestamp_trunc (ifnull(endtime,parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC')),SECOND) end as end_time_utc,
         timestamp_trunc(transactionrequesttime,SECOND) as transaction_request_time_utc,
         timestamp_trunc(transactionresponsetime,SECOND) as transaction_response_time_utc,
         transactionstatus as transaction_status
         from  `sc-neptune-production.permit.permit_exemptions_spukscvs*`
         
        UNION ALL

         SELECT
         sha256(concat(orgID,regionID,siteID,vehicleregistration)) as permit_key,
         orgID as organization,regionID as region,siteID as site_id,vehicleRegistration as plate_no,transactionid as transaction_id,
         case 
         when timestamp_trunc (starttime,SECOND) < timestamp_trunc(transactionresponsetime,SECOND) 
         then timestamp_trunc(transactionresponsetime,SECOND) 
         else timestamp_trunc (starttime,SECOND) end as start_time_utc,
         case 
         when format_timestamp('%Y',endtime) = '2099'
         then parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC') 
         else timestamp_trunc (ifnull(endtime,parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC')),SECOND) end as end_time_utc,
         timestamp_trunc(transactionrequesttime,SECOND) as transaction_request_time_utc,
         timestamp_trunc(transactionresponsetime,SECOND) as transaction_response_time_utc,
         transactionstatus as transaction_status
         from  `sc-neptune-production.permit.permit_exemptions_scmau*`
                  
         UNION ALL

         SELECT
         sha256(concat(orgID,regionID,siteID,vehicleregistration)) as permit_key,
         orgID as organization,regionID as region,siteID as site_id,vehicleRegistration as plate_no,transactionid as transaction_id,
         case 
         when timestamp_trunc (starttime,SECOND) < timestamp_trunc(transactionresponsetime,SECOND) 
         then timestamp_trunc(transactionresponsetime,SECOND) 
         else timestamp_trunc (starttime,SECOND) end as start_time_utc,
         case 
         when format_timestamp('%Y',endtime) = '2099'
         then parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC') 
         else timestamp_trunc (ifnull(endtime,parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC')),SECOND) end as end_time_utc,
         timestamp_trunc(transactionrequesttime,SECOND) as transaction_request_time_utc,
         timestamp_trunc(transactionresponsetime,SECOND) as transaction_response_time_utc,
         transactionstatus as transaction_status
         from  `sc-neptune-production.permit.permit_exemptions_spDenmarkManagedService*`
                 
         UNION ALL

         SELECT
         sha256(concat(orgID,regionID,siteID,vehicleregistration)) as permit_key,
         orgID as organization,regionID as region,siteID as site_id,vehicleRegistration as plate_no,transactionid as transaction_id,
         case 
         when timestamp_trunc (starttime,SECOND) < timestamp_trunc(transactionresponsetime,SECOND) 
         then timestamp_trunc(transactionresponsetime,SECOND) 
         else timestamp_trunc (starttime,SECOND) end as start_time_utc,
         case 
         when format_timestamp('%Y',endtime) = '2099'
         then parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC') 
         else timestamp_trunc (ifnull(endtime,parse_timestamp('%Y-%m-%d %T %Z','9999-12-31 00:00:00 UTC')),SECOND) end as end_time_utc,
         timestamp_trunc(transactionrequesttime,SECOND) as transaction_request_time_utc,
         timestamp_trunc(transactionresponsetime,SECOND) as transaction_response_time_utc,
         transactionstatus as transaction_status
         from  `sc-neptune-production.permit.permit_exemptions_spGermanyManagedService*`
                  
    ),
matched_plates as (
    select * from (
         select 
         orsid as ors_id,
         coalesce(inPlate,outplate) as vrn,
         coalesce(invehiclecolour,outvehiclecolour) as vehicle_colour,
         coalesce(invehicletype,outvehicletype) as vehicle_type,
         timestamp_trunc(arrivalTime,SECOND) as entry_time_utc,
         timestamp_trunc(departureTime,SECOND) as exit_time_utc,
         timestamp_trunc(updateTime,SECOND) as update_time_utc,
         row_number () over (partition by inlpreventid order by updateTime) as rn,
         from `sc-neptune-production.smartcloud.lpr_matching_plates`
         WHERE TIMESTAMP_TRUNC(arrivalTime, DAY) >=TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 6 MONTH)) AND action <> 'deleted'-- added date filter to improve performance
         )
         where rn = 1
         ) 
SELECT 
   s.organization,s.parent, s.groupId, s.GroupName,s.timezone,all_data.vrn,
   datetime(entry_time_utc,s.timezone) as entry_time,
   datetime(exit_time_utc, s.timezone) as exit_time,
   datetime(update_time_utc, s.timezone) as update_time,
   --TIMESTAMP_DIFF(datetime(exit_time_utc, s.timezone),datetime(entry_time_utc,s.timezone), MINUTE) AS stay_duration_in_minutes,
   round((datetime_diff(datetime(exit_time_utc, s.timezone),datetime(entry_time_utc,s.timezone),SECOND))/60,1) as stay_duration_in_minute,
   1 as no_of_visit,
   is_whitelisted
   from 
   (
   SELECT distinct m.*,
   split(m.ors_id,'#')[safe_offset(0)] as orsId,
   split(m.ors_id,'#')[safe_offset(2)] as siteId,
   case 
   when entry_time_utc between p.start_time_utc and p.end_time_utc then TRUE 
   else FALSE end as is_whitelisted
   FROM matched_plates m
   left join permit_all p
   on concat(p.organization,'#',p.region,'#',p.site_id) = m.ors_id
   and p.plate_no=m.vrn
   and entry_time_utc between  p.start_time_utc and p.end_time_utc
   )as all_data 
left join site_all s
on all_data.orsId = s.organization
and all_data.siteId = s.groupId
;

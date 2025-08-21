with sc as (SELECT
'smartcloud' as dataLocation,
dim_site.organization,
dim_site.site_id,
fact_visit.entry_time,
fact_visit.exit_time,
fact_visit.plate_no,
fact_visit.stay_duration_in_minute,
dim_vehicle_indicator.is_whitelisted,
transitionDate,
FORMAT_DATE('%A',dim_entry_date.dim_entry_date) AS Day,
1 as no_of_visit,
lag(entry_time,1,null) over (partition by fact_visit.plate_no,dim_site.site_id order by fact_visit.entry_time) as prev_entry_time,
row_number() over (partition by fact_visit.plate_no order by entry_time desc) as rn,
case 
  when dim_site.organization LIKE '%scmau%' then 'Australia/Queensland'
  when dim_site.organization LIKE '%scm%' then 'Pacific/Auckland'
  when dim_site.organization LIKE '%spGermanyManagedService%' then 'Europe/Berlin'
  when dim_site.organization LIKE '%spukscvs%' then 'Europe/London' 
  when dim_site.organization LIKE '%cityOfMooneeValley%' then 'Australia/Victoria' 
  when dim_site.organization LIKE '%spDenmarkManagedService%' then 'Europe/Copenhagen'  end as timezone,
FROM `sc-neptune-production.managed_services_analytics.fact_visit`  fact_visit
left join `sc-neptune-production.managed_services_analytics.dim_site`  dim_site
on fact_visit.dim_site_key = dim_site.dim_site_key
left join `managed_services_analytics.dim_vehicle` dim_vehicle
on fact_visit.dim_vehicle_key=dim_vehicle.dim_vehicle_key
left join `managed_services_analytics.dim_entry_date` dim_entry_date
on fact_visit.dim_entry_date_key=dim_entry_date.dim_entry_date_key
left join `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
on fact_visit.dim_vehicle_indicator_key=dim_vehicle_indicator.dim_vehicle_indicator_key 
full join `sc-neptune-production.smartcloud.transitionToSmartcloudDate` t ON dim_site.ors_id = t.orsId 
where dim_site.organization = @orgId and dim_site.region = @regionId and dim_site.site_id=@siteId and fact_visit.dim_entry_date between parse_date('%Y%m%d',@startDate) and parse_date('%Y%m%d',@endDate) and dim_entry_date.dim_entry_date >= transitionDate
),
scEntry as (select *,  case when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
else max(rn) end as entrycount 
from sc  
group by datalocation, organization,site_id,entry_time, exit_time,plate_no,stay_duration_in_minute,is_whitelisted,transitionDate, day,no_of_visit,prev_entry_time,rn, timezone
),

timezones as(
SELECT distinct *,  SPLIT(orsId, '#')[SAFE_OFFSET(2)] as site_id, 
case when orsId LIKE '%scmau%' then 'Australia/Queensland'
when orsId LIKE '%scm%' then 'Pacific/Auckland'
when orsId LIKE '%spGermanyManagedService%' then 'Europe/Berlin'
when orsId LIKE '%spukscvs%' then 'Europe/London' 
when orsId LIKE '%cityOfMooneeValley%' then 'Australia/Victoria' 
when orsId LIKE '%spDenmarkManagedService%' then 'Europe/Copenhagen'  end as timezone,
FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
WHERE  DATE(arrivalTime) between date_sub(parse_date('%Y%m%d',@startDate), interval 1 day) and date_add(parse_date('%Y%m%d', @endDate), interval 1 day)
),
sr as (
WITH 
matchingPlates as (
SELECT distinct * ,SPLIT(orsId, '#')[SAFE_OFFSET(0)] as org_id,'smartrep' as dataLocation,
ROW_NUMBER() OVER (PARTITION BY inlpreventid ORDER BY DATETIME(updateTime, timezone) DESC) as rn,
DATETIME(arrivalTime, timezone) as ArrivalDate, 
DATETIME(departureTime, timezone) as DepartureDate,
CASE WHEN arrivaltime IS NOT NULL AND departureTime IS NOT NULL 
THEN ROUND((DATETIME_DIFF(DATETIME(departureTime,timezone),DATETIME(arrivalTime,timezone),SECOND))/60,1) ELSE 0 END AS StayDurationMinutes
FROM timezones 
where date(arrivalTime,timezone) between parse_date('%Y%m%d',@startDate) and parse_date('%Y%m%d',@endDate)
and action != 'deleted' and SPLIT(orsId, '#')[SAFE_OFFSET(0)] = @orgId AND SPLIT(orsId, '#')[SAFE_OFFSET(1)] = @regionId AND SPLIT(orsId, '#')[SAFE_OFFSET(2)] = @siteId
)
select datalocation, matchingPlates.org_id,matchingPlates.site_id, ArrivalDate, DepartureDate, inPlate, StayDurationMinutes,if(inPlate is null,true,false) as dummy, transitionDate, FORMAT_DATE('%A',ArrivalDate) AS Day, 1 as no_of_visit,lag(ArrivalDate,1,NULL) over (partition by inPlate,matchingPlates.orsId order by ArrivalDate) as prev_entry_time,
row_number() over (partition by inplate order by arrivaltime desc) as rn,timezone
from matchingPlates full join `sc-neptune-production.smartcloud.transitionToSmartcloudDate` t ON matchingPlates.orsId = t.orsid 
where rn = 1 and ArrivalDate < transitionDate and 
StayDurationMinutes <= 10000 ),

srEntry as (
  select *, 
       case when date(prev_entry_time) >= date_sub(date(arrivaldate),interval 30 day) then max(rn) + 1 
         else max(rn) end as entrycount 
from sr  
group by datalocation, org_id,site_id, Arrivaldate, DepartureDate,inPlate,StayDurationMinutes,dummy,transitionDate, Day, no_of_visit, prev_entry_time,rn, timezone
)

select datalocation, organization as org_id, site_id, datetime(timestamp(entry_time), timezone) as ArrivalDate,datetime(timestamp(exit_time), timezone) as DepartureDate,plate_no as inPlate,        
      stay_duration_in_minute as StayDurationMinutes, is_whitelisted as dummy,transitionDate,Day, no_of_visit, prev_entry_time, rn, if(entrycount >1, 'repeated','not') as repeatedstatus
from ( select * except(entryCount),max(entryCount) as entryCount, from scEntry 
             group by datalocation, organization, site_id, entry_time, exit_time,plate_no,stay_duration_in_minute,is_whitelisted,transitionDate, day,no_of_visit,prev_entry_time,rn,timezone
     )
union all 
select datalocation, org_id, site_id, ArrivalDate, DepartureDate, inPlate, StayDurationMinutes, dummy, transitionDate, Day, no_of_visit, prev_entry_time, rn,
      if(entrycount >1, 'repeated','not') as repeatedstatus
from ( select * except(entryCount),max(entryCount) as entryCount, from srEntry 
group by datalocation, org_id,site_id, dummy, ArrivalDate, DepartureDate,inPlate,StayDurationMinutes,transitionDate, Day,no_of_visit,prev_entry_time,rn,timezone
 )

where (arrivaldate < transitionDate and datalocation = 'smartrep') 
  or (arrivalDate >= transitionDate and datalocation = 'smartcloud')



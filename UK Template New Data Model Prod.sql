with sc as (SELECT
'smartcloud' as dataLocation,
dim_site.organization,
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

scEntry as (select *, case when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
else max(rn) end as entrycount 
from sc  
group by datalocation, organization,entry_time, exit_time,plate_no,stay_duration_in_minute,is_whitelisted,transitionDate, day,no_of_visit,prev_entry_time,rn
),


sr as (
WITH 
matchingPlates as (
SELECT distinct * ,'smartrep' as dataLocation,
ROW_NUMBER() OVER (PARTITION BY inlpreventid ORDER BY DATETIME(updateTime, 'Europe/London') DESC) as rn,
DATETIME(arrivalTime, 'Europe/London') as ArrivalDate, 
DATETIME(departuretime, 'Europe/London') as DepartureDate,
CASE WHEN arrivaltime IS NOT NULL AND departureTime IS NOT NULL 
THEN ROUND((DATETIME_DIFF(DATETIME(departureTime,'Europe/London'),DATETIME(arrivalTime,'Europe/London'),SECOND))/60,1) ELSE 0 END AS StayDurationMinutes
FROM `sc-neptune-production.smartcloud.lpr_matching_plates_historical`  

where date(arrivaltime,'Europe/London') between parse_date('%Y%m%d',@startDate) and parse_date('%Y%m%d',@endDate)
and action != 'deleted' and SPLIT(orsId, '#')[SAFE_OFFSET(0)] = @orgId AND SPLIT(orsId, '#')[SAFE_OFFSET(1)] = @regionId AND SPLIT(orsId, '#')[SAFE_OFFSET(2)] = @siteId
)
select datalocation, matchingPlates.orsId, arrivaldate, departureDate, inplate, stayDurationMinutes,if(inPlate is null,true,false) as dummy, transitionDate, 
FORMAT_DATE('%A',arrivalDate) AS Day, 1 as no_of_visit,lag(arrivaldate,1,NULL) over (partition by inplate,matchingPlates.orsId order by arrivaldate) as prev_entry_time,
row_number() over (partition by inplate order by arrivaltime desc) as rn
from matchingPlates full join `sc-neptune-production.smartcloud.transitionToSmartcloudDate` t ON matchingPlates.orsid = t.orsid 
where rn = 1 and arrivalDate < transitionDate and 
stayDurationMinutes <= 10000 ),

srEntry as (select *, case when date(prev_entry_time) >= date_sub(date(arrivaldate),interval 30 day) then max(rn) + 1 
else max(rn) end as entrycount 
from sr  
group by datalocation, orsId,arrivaldate, departuredate,inplate,stayDurationMinutes,dummy,transitionDate, day,no_of_visit,prev_entry_time,rn
)

select *,if(entrycount >1, 'repeated','not') as repeatedstatus
from ( select * except(entryCount),max(entryCount) as entryCount, from scEntry 
group by datalocation, organization,entry_time, exit_time,plate_no,stay_duration_in_minute,is_whitelisted,transitionDate, day,no_of_visit,prev_entry_time,rn
 )
union all 
select *,if(entrycount >1, 'repeated','not') as repeatedstatus
from ( select * except(entryCount),max(entryCount) as entryCount, from srEntry 
group by datalocation, orsId, dummy, arrivaldate, departuredate,inplate,staydurationminutes,transitionDate, day,no_of_visit,prev_entry_time,rn
 )

where case when arrivaldate < transitionDate then datalocation = 'smartrep' when arrivalDate >= transitionDate then datalocation = 'smartcloud' end
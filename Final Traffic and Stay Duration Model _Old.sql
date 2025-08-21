with current_month as (
   SELECT
      dim_site.site_id,
      dim_site.site_name,
      dim_site.timezone,
      dim_vehicle_indicator.is_whitelisted,
      fact_visit.stay_duration_in_minute,
      fact_visit.plate_no,
      fact_visit.entry_time,
      fact_visit.exit_time,
      fact_visit.no_of_visit,
      dim_duration_of_stay_by_minute.0_to_2_minute_interval,
      dim_duration_of_stay_by_minute.0_to_15_minute_interval,
      dim_duration_of_stay_by_minute.0_to_30_minute_interval,
      dim_entry_date.dim_entry_date,
      dim_entry_time.dim_entry_time_hour_bucket,
      dim_entry_date.dim_entry_date_calendar_first_day_of_month,
      dim_entry_date.dim_entry_date_calendar_last_day_of_month,
      dim_entry_date.dim_entry_date_calendar_month_number,
      lag(entry_time,1,NULL) over (partition by fact_visit.plate_no,dim_site.site_id order by fact_visit.entry_time) as prev_entry_time,
      date_add(dim_entry_date_calendar_last_day_of_month,interval 1 day) as next_month,
      case 
	     when is_whitelisted = True and dim_entry_date_calendar_first_day_of_month = DATETIME_TRUNC(DATE(CAST(SUBSTR(@startDate, 1, 4) AS INT64),
                                                                                                         CAST(SUBSTR(@startDate, 5, 2) AS INT64),
                                                           CAST(SUBSTR(@startDate, 7, 2) AS INT64)), month) then SUM(no_of_visit) OVER (PARTITION BY fact_visit.plate_no) end AS TotalVisits,
row_number() over (partition by fact_visit.plate_no order by entry_time desc) as rn,
dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number


FROM `sc-neptune-production.managed_services_analytics.fact_visit`  fact_visit
left join `sc-neptune-production.managed_services_analytics.dim_site`  dim_site
on fact_visit.dim_site_key = dim_site.dim_site_key
left join `managed_services_analytics.dim_duration_of_stay_by_minute` dim_duration_of_stay_by_minute
on fact_visit.dim_duration_of_stay_by_minute_key= dim_duration_of_stay_by_minute.dim_duration_of_stay_by_minute_key
left join `managed_services_analytics.dim_vehicle` dim_vehicle
on fact_visit.dim_vehicle_key=dim_vehicle.dim_vehicle_key
left join `managed_services_analytics.dim_entry_date` dim_entry_date
on fact_visit.dim_entry_date_key=dim_entry_date.dim_entry_date_key
left join `managed_services_analytics.dim_exit_date` dim_exit_date
on fact_visit.dim_exit_date_key=dim_exit_date.dim_exit_date_key
left join `managed_services_analytics.dim_entry_time` dim_entry_time
on fact_visit.dim_entry_time_key=dim_entry_time.dim_entry_time_key
left join `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
on fact_visit.dim_vehicle_indicator_key=dim_vehicle_indicator.dim_vehicle_indicator_key
where dim_site.site_id=@siteId and fact_visit.dim_entry_date between DATE_SUB(PARSE_DATE('%Y%m%d', @startDate),INTERVAL 1 MONTH) and PARSE_DATE('%Y%m%d',@endDate) 

),
entryCurrent as (select *, case when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
else max(rn) end as entrycount 
from current_month  
group by plate_no,prev_entry_time,entry_time,site_id,site_name,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,`0_to_2_minute_interval`,`0_to_15_minute_interval`, `0_to_30_minute_interval`,dim_entry_date_calendar_first_day_of_month, dim_entry_date_calendar_last_day_of_month,dim_entry_date_calendar_month_number,next_month,totalvisits,rn,
dim_entry_date,dim_entry_time_hour_bucket, plate_no,dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number),
previous_month as (SELECT
dim_site.site_id,
dim_site.site_name,
dim_site.timezone,
dim_vehicle_indicator.is_whitelisted,
fact_visit.stay_duration_in_minute,
fact_visit.plate_no,
fact_visit.entry_time,
fact_visit.exit_time,
fact_visit.no_of_visit,
dim_duration_of_stay_by_minute.0_to_2_minute_interval,
dim_duration_of_stay_by_minute.0_to_15_minute_interval,
dim_duration_of_stay_by_minute.0_to_30_minute_interval,
dim_entry_date.dim_entry_date,
dim_entry_time.dim_entry_time_hour_bucket,
dim_entry_date.dim_entry_date_calendar_first_day_of_month,
dim_entry_date.dim_entry_date_calendar_last_day_of_month,
dim_entry_date.dim_entry_date_calendar_month_number,
lag(entry_time,1,NULL) over (partition by fact_visit.plate_no,dim_site.site_id order by fact_visit.entry_time) as prev_entry_time,
date_add(dim_entry_date_calendar_last_day_of_month,interval 1 day) as next_month,
case when is_whitelisted = True and dim_entry_date_calendar_first_day_of_month = DATETIME_TRUNC(DATE(CAST(SUBSTR(@startDate, 1, 4) AS INT64),
  CAST(SUBSTR(@startDate, 5, 2) AS INT64),
  CAST(SUBSTR(@startDate, 7, 2) AS INT64)), month) then SUM(no_of_visit) OVER (PARTITION BY fact_visit.plate_no) end AS TotalVisits,
row_number() over (partition by fact_visit.plate_no order by entry_time desc) as rn,
dim_entry_date_day_of_week_description,
dim_entry_date_calendar_week_number



FROM `sc-neptune-production.managed_services_analytics.fact_visit`  fact_visit
left join `sc-neptune-production.managed_services_analytics.dim_site`  dim_site
on fact_visit.dim_site_key = dim_site.dim_site_key
left join `managed_services_analytics.dim_duration_of_stay_by_minute` dim_duration_of_stay_by_minute
on fact_visit.dim_duration_of_stay_by_minute_key= dim_duration_of_stay_by_minute.dim_duration_of_stay_by_minute_key
left join `managed_services_analytics.dim_vehicle` dim_vehicle
on fact_visit.dim_vehicle_key=dim_vehicle.dim_vehicle_key
left join `managed_services_analytics.dim_entry_date` dim_entry_date
on fact_visit.dim_entry_date_key=dim_entry_date.dim_entry_date_key
left join `managed_services_analytics.dim_exit_date` dim_exit_date
on fact_visit.dim_exit_date_key=dim_exit_date.dim_exit_date_key
left join `managed_services_analytics.dim_entry_time` dim_entry_time
on fact_visit.dim_entry_time_key=dim_entry_time.dim_entry_time_key
left join `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
on fact_visit.dim_vehicle_indicator_key=dim_vehicle_indicator.dim_vehicle_indicator_key
where dim_site.site_id=@siteId   
and 
(dim_entry_date_calendar_first_day_of_month = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 2 month),month) or dim_entry_date_calendar_first_day_of_month = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 month),month) )

),
entryPreviousMonth as (select *, case when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
else max(rn) end as entrycount 
from previous_month  where dim_entry_date_calendar_first_day_of_month = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 month),month)
group by plate_no,prev_entry_time,entry_time,site_id,site_name,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,`0_to_2_minute_interval`,`0_to_15_minute_interval`, `0_to_30_minute_interval`,dim_entry_date_calendar_first_day_of_month, dim_entry_date_calendar_last_day_of_month,dim_entry_date_calendar_month_number,next_month,totalvisits,rn,
dim_entry_date,dim_entry_time_hour_bucket, plate_no,dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number),


previous_year as (SELECT
dim_site.site_id,
dim_site.site_name,
dim_site.timezone,
dim_vehicle_indicator.is_whitelisted,
fact_visit.stay_duration_in_minute,
fact_visit.plate_no,
fact_visit.entry_time,
fact_visit.exit_time,
fact_visit.no_of_visit,
dim_duration_of_stay_by_minute.0_to_2_minute_interval,
dim_duration_of_stay_by_minute.0_to_15_minute_interval,
dim_duration_of_stay_by_minute.0_to_30_minute_interval,
dim_entry_date.dim_entry_date,
dim_entry_time.dim_entry_time_hour_bucket,
dim_entry_date.dim_entry_date_calendar_first_day_of_month,
dim_entry_date.dim_entry_date_calendar_last_day_of_month,
dim_entry_date.dim_entry_date_calendar_month_number,
lag(entry_time,1,NULL) over (partition by fact_visit.plate_no,dim_site.site_id order by fact_visit.entry_time) as prev_entry_time,
date_add(dim_entry_date_calendar_last_day_of_month,interval 1 day) as next_month
,case when is_whitelisted = True and dim_entry_date_calendar_first_day_of_month = DATETIME_TRUNC(DATE(CAST(SUBSTR(@startDate, 1, 4) AS INT64),
  CAST(SUBSTR(@startDate, 5, 2) AS INT64),
  CAST(SUBSTR(@startDate, 7, 2) AS INT64)), month) then SUM(no_of_visit) OVER (PARTITION BY fact_visit.plate_no) end AS TotalVisits
,row_number() over (partition by fact_visit.plate_no order by entry_time desc) as rn ,
dim_entry_date_day_of_week_description,
dim_entry_date_calendar_week_number


FROM `sc-neptune-production.managed_services_analytics.fact_visit`  fact_visit
left join `sc-neptune-production.managed_services_analytics.dim_site`  dim_site
on fact_visit.dim_site_key = dim_site.dim_site_key
left join `managed_services_analytics.dim_duration_of_stay_by_minute` dim_duration_of_stay_by_minute
on fact_visit.dim_duration_of_stay_by_minute_key= dim_duration_of_stay_by_minute.dim_duration_of_stay_by_minute_key
left join `managed_services_analytics.dim_vehicle` dim_vehicle
on fact_visit.dim_vehicle_key=dim_vehicle.dim_vehicle_key
left join `managed_services_analytics.dim_entry_date` dim_entry_date
on fact_visit.dim_entry_date_key=dim_entry_date.dim_entry_date_key
left join `managed_services_analytics.dim_exit_date` dim_exit_date
on fact_visit.dim_exit_date_key=dim_exit_date.dim_exit_date_key
left join `managed_services_analytics.dim_entry_time` dim_entry_time
on fact_visit.dim_entry_time_key=dim_entry_time.dim_entry_time_key
left join `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
on fact_visit.dim_vehicle_indicator_key=dim_vehicle_indicator.dim_vehicle_indicator_key
where dim_site.site_id=@siteId and dim_entry_date.dim_entry_date_calendar_first_day_of_month = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 year), month)
),
entryPreviousYear as (select *, case when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
else max(rn) end as entrycount 
from previous_year  
group by plate_no,prev_entry_time,entry_time,site_id,site_name,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,`0_to_2_minute_interval`,`0_to_15_minute_interval`, `0_to_30_minute_interval`,dim_entry_date_calendar_first_day_of_month, dim_entry_date_calendar_last_day_of_month,dim_entry_date_calendar_month_number,next_month,totalvisits,rn,
dim_entry_date,dim_entry_time_hour_bucket, plate_no,dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number)


select *,if(entrycount >1, 'repeated','not') as repeatedstatus
from ( select * except(entryCount),max(entryCount) as entryCount,'current_month' as source from entryCurrent where dim_entry_date between PARSE_DATE('%Y%m%d', @startDate) and PARSE_DATE('%Y%m%d',@endDate) 
group by plate_no,prev_entry_time,entry_time,site_id,site_name,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,`0_to_2_minute_interval`,`0_to_15_minute_interval`, `0_to_30_minute_interval`,dim_entry_date_calendar_first_day_of_month, dim_entry_date_calendar_week_number,dim_entry_date_calendar_last_day_of_month,dim_entry_date_calendar_month_number,next_month,totalvisits,rn,dim_entry_date_day_of_week_description,
dim_entry_date,dim_entry_time_hour_bucket, plate_no 

union all 

select * except(entrycount),max(entryCount) as entryCount, 'previous_month' as source,
 
 from entryPreviousMonth where dim_entry_date_calendar_first_day_of_month = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 month), month) group by dim_entry_date_calendar_week_number,plate_no,prev_entry_time,entry_time,site_id,site_name,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,`0_to_2_minute_interval`,`0_to_15_minute_interval`, `0_to_30_minute_interval`,dim_entry_date_calendar_first_day_of_month, dim_entry_date_calendar_last_day_of_month,dim_entry_date_calendar_month_number,next_month,totalvisits,rn,dim_entry_date_day_of_week_description,
dim_entry_date,dim_entry_time_hour_bucket, plate_no
 
 
 union all select * except(entrycount),max(entryCount) as entryCount,'previous_year' as source
from entryPreviousYear
  group by plate_no,prev_entry_time,entry_time,site_id,site_name,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,`0_to_2_minute_interval`,`0_to_15_minute_interval`, `0_to_30_minute_interval`,dim_entry_date_calendar_first_day_of_month, dim_entry_date_calendar_last_day_of_month,dim_entry_date_calendar_month_number,next_month,totalvisits,rn,dim_entry_date_day_of_week_description,
dim_entry_date,dim_entry_time_hour_bucket, plate_no,dim_entry_date_calendar_week_number
 ) 
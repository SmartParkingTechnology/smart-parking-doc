SELECT
dim_site.site_id,
dim_site.site_name,
fact_visit.no_of_visit,
dim_entry_date.dim_entry_date,
dim_entry_date.dim_entry_date_calendar_month_number, 
dim_entry_date.dim_entry_date_calendar_week_number,
dim_entry_date_calendar_first_day_of_month,
dim_vehicle_indicator.is_whitelisted

 
FROM `sc-neptune-production.managed_services_analytics.fact_visit`  fact_visit
left join `sc-neptune-production.managed_services_analytics.dim_site`  dim_site
on fact_visit.dim_site_key = dim_site.dim_site_key
left join `managed_services_analytics.dim_duration_of_stay_by_minute` dim_duration_of_stay_by_minute
on fact_visit.dim_duration_of_stay_by_minute_key= dim_duration_of_stay_by_minute.dim_duration_of_stay_by_minute_key
--left join `managed_services_analytics.dim_vehicle` dim_vehicle
--on fact_visit.dim_vehicle_key=dim_vehicle.dim_vehicle_key
left join `managed_services_analytics.dim_entry_date` dim_entry_date
on fact_visit.dim_entry_date_key=dim_entry_date.dim_entry_date_key
left join `managed_services_analytics.dim_exit_date` dim_exit_date
on fact_visit.dim_exit_date_key=dim_exit_date.dim_exit_date_key
left join `managed_services_analytics.dim_entry_time` dim_entry_time
on fact_visit.dim_entry_time_key=dim_entry_time.dim_entry_time_key
left join `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
on fact_visit.dim_vehicle_indicator_key=dim_vehicle_indicator.dim_vehicle_indicator_key 
full join `sc-neptune-production.smartcloud.transitionToSmartcloudDate` t ON dim_site.ors_id = t.orsId 

where fact_visit.organization = @orgId and fact_visit.region = @regionId and dim_site.site_id=@siteId and fact_visit.dim_entry_date between date_sub(parse_date('%Y%m%d',concat(extract(year from parse_date('%Y%m%d', @startDate)),'0101')), interval 1 year) and parse_date('%Y%m%d',concat(extract(year from parse_date('%Y%m%d', @endDate)),'1231')) 
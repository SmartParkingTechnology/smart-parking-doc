with allData as(
SELECT
fact_visit.organization,
fact_visit.site_id,
fact_visit.timezone,
dim_vehicle_indicator.is_whitelisted,
fact_visit.stay_duration_in_minute,
fact_visit.plate_no,
fact_visit.entry_time,
fact_visit.exit_time,
fact_visit.no_of_visit,
fact_visit.dim_entry_date,
   (
      SELECT MAX(prev_fact_visit.entry_time)
      FROM `sc-neptune-production.managed_services_analytics.fact_visit` prev_fact_visit
      WHERE prev_fact_visit.plate_no = fact_visit.plate_no
      AND prev_fact_visit.dim_site_key = fact_visit.dim_site_key
      AND prev_fact_visit.entry_time < fact_visit.entry_time
    ) AS prev_entry_time,
case when is_whitelisted = True and datetime_trunc(fact_visit.dim_entry_date, month) = DATETIME_TRUNC(DATE(CAST(SUBSTR(@startDate, 1, 4) AS INT64),
                                                                                                           CAST(SUBSTR(@startDate, 5, 2) AS INT64),
                                                                                                           CAST(SUBSTR(@startDate, 7, 2) AS INT64)), month) 
     then SUM(no_of_visit) OVER (PARTITION BY fact_visit.plate_no) end AS TotalVisits,
row_number() over (partition by fact_visit.plate_no order by entry_time desc) as rn,
dim_entry_date_day_of_week_description,
dim_entry_date_calendar_week_number
  
FROM `sc-neptune-production.managed_services_analytics.fact_visit`  fact_visit
left join `managed_services_analytics.dim_entry_date` dim_entry_date
on fact_visit.dim_entry_date_key=dim_entry_date.dim_entry_date_key
left join `managed_services_analytics.dim_entry_time` dim_entry_time
on fact_visit.dim_entry_time_key=dim_entry_time.dim_entry_time_key
left join `managed_services_analytics.dim_vehicle_indicator` dim_vehicle_indicator
on fact_visit.dim_vehicle_indicator_key=dim_vehicle_indicator.dim_vehicle_indicator_key
  where fact_visit.site_id=@siteId
),
current_month AS(
select * from allData a
where site_id=@siteId and dim_entry_date between DATE_SUB(PARSE_DATE('%Y%m%d', @startDate),INTERVAL 1 MONTH) and PARSE_DATE('%Y%m%d',@endDate) 
  ),
entryCurrent AS (
  SELECT *,
    CASE
      WHEN DATE(prev_entry_time) >= DATE_SUB(DATE(entry_time), INTERVAL 30 DAY) THEN MAX(rn) + 1
      ELSE MAX(rn) END AS entrycount
  FROM current_month
  GROUP BY plate_no, prev_entry_time, entry_time, site_id, timezone, is_whitelisted, stay_duration_in_minute, entry_time, exit_time, no_of_visit,  TotalVisits, rn, dim_entry_date,                  dim_entry_date_day_of_week_description, dim_entry_date_calendar_week_number
),
previous_month as(
  select * from allData a
  where site_id= @siteId
  and 
  (date_trunc(dim_entry_date, month) = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 2 month),month) 
    or date_trunc(dim_entry_date, month) = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 month),month) )),

entryPreviousMonth AS (
  select *, 
    case when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
    else max(rn) end as entrycount 
  from previous_month  where dim_entry_date_calendar_first_day_of_month = date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 month),month)
  group by plate_no,prev_entry_time,entry_time,site_id,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,totalvisits,rn,dim_entry_date,      
           dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number
),
previous_year AS(
  select * from allData a
  where site_id= @siteId 
  and date_trunc(dim_entry_date, month)= date_trunc(date_sub(date(parse_date('%Y%m%d', @startDate)), interval 1 year), month)),

entryPreviousYear AS (
  select *, 
   case 
     when date(prev_entry_time) >= date_sub(date(entry_time),interval 30 day) then max(rn) + 1 
     else max(rn) end as entrycount 
  from previous_year  
  group by plate_no,prev_entry_time,entry_time,site_id,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,totalvisits,rn,
           dim_entry_date, dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number
)

SELECT *,
  IF(entrycount > 1, 'repeated', 'not') AS repeatedstatus
FROM (  -- Combine current, previous month, and previous year
  
    SELECT * EXCEPT(entrycount),
    MAX(entryCount) AS entryCount,
    'current_month' AS source
  FROM entryCurrent
  WHERE dim_entry_date BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate)
  GROUP BY plate_no,prev_entry_time,entry_time,site_id,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,totalvisits,rn,
           dim_entry_date, dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number

  UNION ALL
  
  SELECT * EXCEPT(entrycount),
    MAX(entryCount) AS entryCount,
    'previous_month' AS source
  FROM entryPreviousMonth
  WHERE date_trunc(dim_entry_date, month) = DATE_TRUNC(DATE_SUB(DATE(PARSE_DATE('%Y%m%d', @startDate)), INTERVAL 1 MONTH), MONTH)
  GROUP BY plate_no,prev_entry_time,entry_time,site_id,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,totalvisits,rn,
           dim_entry_date,dim_entry_time_hour_bucket, dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number

  
  UNION ALL
  
  SELECT * EXCEPT(entrycount),
    MAX(entryCount) AS entryCount,
    'previous_year' AS source
  FROM entryPreviousYear
  GROUP BY plate_no,prev_entry_time,entry_time,site_id,timezone,is_whitelisted,stay_duration_in_minute,entry_time,exit_time,no_of_visit,totalvisits,rn,
           dim_entry_date,dim_entry_time_hour_bucket, dim_entry_date_day_of_week_description,dim_entry_date_calendar_week_number
)

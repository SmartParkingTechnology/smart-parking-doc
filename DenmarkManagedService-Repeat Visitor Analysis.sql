WITH alldata AS (WITH dataa AS (
  SELECT 
  dim_site.site_id,
  site_name,
  ors_Id,
  dim_entry_date_calendar_first_day_of_month,
    fact_visit.plate_no,
    entry_time,
    LAG(entry_time,1,NULL) OVER (PARTITION BY fact_visit.plate_no,dim_site.site_id ORDER BY fact_visit.entry_time) AS prev_entry_time
  FROM `sc-neptune-production.managed_services_analytics.fact_visit` AS fact_visit
  LEFT JOIN `sc-neptune-production.managed_services_analytics.dim_site` AS dim_site
    ON fact_visit.dim_site_key = dim_site.dim_site_key
  LEFT JOIN `managed_services_analytics.dim_entry_date` AS dim_entry_date
    ON fact_visit.dim_entry_date_key = dim_entry_date.dim_entry_date_key
  LEFT JOIN `managed_services_analytics.dim_vehicle_indicator` AS dim_vehicle_indicator
    ON fact_visit.dim_vehicle_indicator_key = dim_vehicle_indicator.dim_vehicle_indicator_key
  WHERE dim_site.organization = 'spDenmarkManagedService' AND 
     DATE_TRUNC(dim_entry_date.dim_entry_date_calendar_first_day_of_month, MONTH) = 
          DATE_TRUNC(DATE(CAST(SUBSTR(@ds_start_date, 1, 4) AS INT64),
                          CAST(SUBSTR(@ds_start_date, 5, 2) AS INT64),
                          CAST(SUBSTR(@ds_start_date, 7, 2) AS INT64)), MONTH)

     OR DATE_TRUNC(dim_entry_date.dim_entry_date_calendar_first_day_of_month, MONTH) = 
          DATE_TRUNC(date_sub(DATE(CAST(SUBSTR(@ds_start_date, 1, 4) AS INT64),
                          CAST(SUBSTR(@ds_start_date, 5, 2) AS INT64),
                          CAST(SUBSTR(@ds_start_date, 7, 2) AS INT64)), interval 1 MONTH), MONTH)) --need - 1 month for the lag 

SELECT 
  *, ROW_NUMBER() OVER (PARTITION BY ors_Id,plate_no ORDER BY entry_time DESC) AS rn 
FROM dataa WHERE date(dataa.entry_time) BETWEEN parse_date('%Y%m%d',@ds_start_date) and parse_date('%Y%m%d',@ds_end_date) 
),

more AS (SELECT site_name,site_id,entry_time, prev_entry_time, CASE WHEN date(prev_entry_time) >= DATE_SUB(date(entry_time),interval 30 day) then max(rn) + 1 
ELSE MAX(rn) END AS entrycount, plate_no 
FROM alldata 
GROUP BY plate_no,prev_entry_time,entry_time,site_id,site_name)


SELECT site_name, site_id, entry_time, prev_entry_time, entrycount, plate_no,if(entrycount >1, 'repeated','not') AS repeatedstatus

FROM (SELECT site_name,site_id,MAX(entrycount) AS entrycount,entry_time,prev_entry_time, plate_no FROM more GROUP BY site_id,site_name,plate_no,entry_time,prev_entry_time)
WHERE entrycount IS NOT null
ORDER BY plate_no ASC 
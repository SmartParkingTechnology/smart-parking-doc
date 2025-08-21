with matchingplates AS(
SELECT distinct *,
SPLIT(orsId, '#')[SAFE_OFFSET(2)] as siteId,
ROW_NUMBER() OVER (PARTITION BY inlpreventid ORDER BY DATETIME(updateTime, 'Europe/London') DESC) as rn,
DATETIME(arrivalTime, 'Europe/London') as ArrivalDate, 
DATETIME(departuretime, 'Europe/London') as DepartureDate,
CASE WHEN arrivaltime IS NOT NULL AND departureTime IS NOT NULL 
THEN ROUND((DATETIME_DIFF(DATETIME(departureTime,'Europe/London'),DATETIME(arrivalTime,'Europe/London'),SECOND))/60,1) ELSE 0 END AS StayDurationMinutes
FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
WHERE  DATE(arrivalTime) between (parse_date('%Y%m%d','20240401')) and (parse_date('%Y%m%d', '20240407'))
and SPLIT(orsId, '#')[SAFE_OFFSET(2)] = 'graysShoppingCentre'
and action!= 'deleted'
)
/*,
SiteName as
(
  SELECT ors_id, site_id, site_name
      FROM `sc-neptune-production.managed_services_analytics.dim_site`
)*/
select * from matchingplates 
--inner join SiteName s
--on m.siteId = s.site_id
where rn=1
and stayDurationMinutes <= 10000
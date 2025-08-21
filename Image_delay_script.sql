WITH siteGroups as (
select organization as org,region, site_id , site_name as name
FROM  `sc-neptune-production.managed_services_analytics.dim_site`
where organization = 'spukscvs'
),
delays as (
SELECT orsId, count(*) as numberOfEvents, avg(DATETIME_DIFF(receivedTimestamp, fact.timestamp, MINUTE)) as avgD, min(DATETIME_DIFF(receivedTimestamp, fact.timestamp, MINUTE)) as minD, max(DATETIME_DIFF(receivedTimestamp, fact.timestamp, MINUTE)) as maxD FROM `sc-neptune-production.smartcloud.lpr_events_v2` 
WHERE DATE(timestamp, 'Europe/London') between PARSE_DATE('%Y%m%d',@DS_START_DATE) AND PARSE_DATE('%Y%m%d',@DS_END_DATE)
and starts_with(orsId, 'spukscvs')
and receivedTimestamp is not null 
group by orsId
)

select name, numberOfEvents, minD, avgD, maxD  from siteGroups s
left join delays d
on d.orsId = CONCAT('spukscvs#', s.region, '#', s.site_id)
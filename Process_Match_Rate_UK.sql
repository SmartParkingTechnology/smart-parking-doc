WITH siteGroups as (
select parent, groupId, name
FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
where type = 'site'
group by key
)
AND actionType != 'GroupDeleted'
),
processMatch As (
SELECT # the total and rejected counts are x 2 because the relevant missing of out needed to be assumed to appear in the IN counts are well
sum(accepted) as acceptedCount, sum(rejected) as rejectedCount, sum(total) as totalCount, 
  
DATE(startProcessingTime, 'Europe/London') as processingDate, org, region, site
FROM `sc-neptune-production.smartcloud.process_metrics`
WHERE DATE(startProcessingTime, 'Europe/London') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)-- AND DATE(startProcessingTime, 'Europe/London') > '2023-02-20'  # this table was only deployed after 20th Feb
group by processingDate, org, region, site
),
rawMatch As (
SELECT 
DATE(timestamp, 'Europe/London') as rawMatchDate,
SUM(CAST(entry as INT64)) as entries,
SUM(IF(CAST(entry as INT64) = 0, 1, 0)) as exits,
g.key as gKey,
 FROM `sc-neptune-production.lpr_events.lpr_events_spukscvs`, unnest(`groups`) g
WHERE DATE(timestamp, 'Europe/London') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
group by g.key, DATE(timestamp, 'Europe/London')
)

select
name,
entries,
exits,
DATETIME(rawMatchDate) as matchDate,
ROUND(acceptedCount / totalCount, 2) AS processMatchRate,
ROUND(LEAST(exits / ((entries + exits) / 2), entries / ((entries + exits) / 2)), 2) as rawMatchRate,
totalCount as footFall
from processMatch
join rawMatch ON
processingDate = rawMatchDate AND gKey = CONCAT(org, '#', region, '#', site)
inner join siteGroups ON
parent = region AND groupId = site
where totalCount != 0 and org = 'spukscvs'
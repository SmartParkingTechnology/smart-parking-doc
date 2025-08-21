WITH sites as (
select DISTINCT groupId, name, actionType,timestamp,organization,parent

FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
where groupId = @siteId AND parent = @regionId 
  --_TABLE_SUFFIX = @orgId AND 
 and type = 'site'
group by key
)
AND actionType != 'GroupDeleted'
),
finances as (
select * from(
SELECT org,time,transactionType,breachId, description, 
CASE WHEN debit >= 875 THEN 875 ELSE 0 END as debit,
site,region,
'Europe/Copenhagen' AS timezone
FROM `smartcloud.contravention_financials` 
  where date(time)>=date('2024-01-01') 
  and org = 'spDenmarkManagedService'
)
 WHERE org = 'spDenmarkManagedService' AND DATE(time,timezone) BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d', @endDate))

select f.* , s.name from finances f
inner join sites s ON
f.org = s.organization AND f.region = s.parent AND f.site = s.groupId
where s.groupId = @siteId and debit != 0
 
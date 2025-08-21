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
SELECT org,site,region,time,transactionType,breachId, description, credit,
CASE 
WHEN credit >= 875 
THEN 875 
ELSE 0 END as amount,
datetime(time,'Europe/Copenhagen' ) as updateTime, 
'Europe/Copenhagen' AS timezone,
row_number() over (partition by site,breachId order by time asc) as rn
FROM `smartcloud.contravention_financials` 
  where date(time)>=date('2024-01-01') 
  and org = 'spDenmarkManagedService'
)
 WHERE DATETIME(time,timezone) BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d', @endDate)
and rn = 1)

select f.*, s.name from finances f
inner join sites s ON
f.org = s.organization AND f.region = s.parent AND f.site = s.groupId
where s.groupId = @siteId and credit!= 0
 
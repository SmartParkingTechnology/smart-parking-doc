SELECT distinct * FROM `sc-neptune-production.smartcloud.contravention_financials` WHERE TIMESTAMP_TRUNC(time, DAY) > TIMESTAMP("2024-01-11")
and org = 'spDenmarkManagedService' 
and siteLocation like '%1017%' 
order by time desc LIMIT 1000




SELECT distinct *
 FROM `sc-neptune-production.group_actions.group_actions_*` 
where organization = 'spDenmarkManagedService'
and name like '%1017%' 
order by timestamp desc LIMIT 1000




WITH sites as (
select DISTINCT groupId, name, actionType,timestamp,organization, parent
/*case 
  when name = 'Lyskær 8, 2730 Herlev - Plads (1001)' then 'Plads 1001 - Lyskær 8, 2730 Herlev'
  when name = 'Lyskær 11, 2730 Herlev - (Plads 1002)' then 'Plads 1002 - Lyskær 11, 2730 Herlev'
  when name = 'Rugvænget 19-21, 2630 Taastrup - (Plads 1003)' then 'Plads 1003 - Rugvænget 19-21, 2630 Taastrup'
  when name = 'Islevdalvej 214, 2610 Rødovre - (Plads 1004)' then 'Plads 1004 - Islevdalvej 214, 2610 Rødovre'
  when name = 'Østergade 1, 3600 Frederikssund - (Plads 1005)' then 'Plads 1005 - Østergade 1, 3600 Frederikssund'
  when name = 'Fonnesbechsgade 20, 7400 Herning - (Plads 1011)' then 'Plads 1011 - Fonnesbechsgade 20, 7400 Herning'
  when name = 'Jernbanegade 6, 9800 Hjørring - (Plads 1013)' then 'Plads 1013 - Jernbanegade 6, 9800 Hjørring'
  when name = 'Parkvej 7-15, 2630 Taastrup - (Plads 1015)' then 'Plads 1015 - Parkvej 7-15, 2630 Taastrup'
  when name = 'Parkvej 7-15, 2630 Taastrup - (Plads 1016)' then 'Plads 1016 - Parkvej 7-15, 2630 Taastrup'
  when name = 'Søndre Jernbanevej 28, 3400 HIllerød - (Plads 1017)' then 'Plads 1017 - Søndre Jernbanevej 28, 3400 HIllerød'
  when name = 'Fredensgade 4A, 9330 Dronninglund - (Plads 1019)' then 'Plads 1019 - Fredensgade 4A, 9330 Dronninglund'
  when name = 'Smart Parking Office - (Plads nr. 9999)' then 'Smart Parking Office'
else name end as SiteName*/
FROM `sc-neptune-production.group_actions.group_actions_*`

where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_*`
where 
#groupId like '%plads1017%' AND parent like '%gefion%' 
  --_TABLE_SUFFIX = @orgId AND 
# and 
 type = 'site'
group by key
)
AND actionType != 'GroupDeleted'
),
finances as (
select * from(
SELECT org,time,transactionType,breachId, description, debit,region,site,
CASE 
WHEN org = 'scmau' THEN 'Australia/Queensland' 
WHEN org = 'scm' THEN 'Pacific/Auckland' 
WHEN org = 'spukscvs' THEN 'Europe/London' 
WHEN org = 'spGermanyManagedService' THEN 'Europe/Berlin'
WHEN org = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' WHEN org = 'cityOfMooneeValley' THEN 'Australia/Victoria' END AS timezone  
FROM `smartcloud.contravention_financials` 
  where date(time)>=date('2024-01-01') 
  and org = 'spDenmarkManagedService'
)
)-- WHERE org = @orgId AND DATE(time,timezone) BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d', @endDate))

select f.* from finances f
inner join sites s ON
f.org = s.organization AND f.region = s.parent AND f.site = s.groupId
where s.groupId like '%plads1013%'
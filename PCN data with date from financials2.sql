with pcn as (
select distinct * from(
    SELECT 
        breaches.ticketId AS ticketId, 
        fees.offense.type AS offenseType,  
        fees.amount AS feeAmount,            
        fees.source AS feeSource,
        DATETIME(fact.timestamp, 'Europe/Copenhagen') AS latestUpdateTime,  
        state AS caseState,
        subState AS caseSubState,
        organization,
        g.name AS Site,
        g.groupref,
        ROW_NUMBER() OVER (PARTITION BY g.groupref,breaches.ticketId ORDER BY fact.timestamp DESC) AS rn
    FROM 
        `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService`,
        UNNEST(breaches) AS breaches,        
        UNNEST(breaches.fees) AS fees,
        UNNEST(`groups`) AS g        
    WHERE 
        fees.offense.type NOT IN ('LateFee', 'CompensationFee')  
        AND fees.source = 'Smartcloud'   
        AND state = 'Closed'
        order by ticketId
) AS subquery
WHERE rn = 1
order by ticketId
),
finance as(
SELECT org,time,transactionType,breachId, description, debit, site,region,
row_number() over(partition by site,breachId order by time desc) as row_num,
CASE 
WHEN org = 'scmau' THEN 'Australia/Queensland' 
WHEN org = 'scm' THEN 'Pacific/Auckland' 
WHEN org = 'spukscvs' THEN 'Europe/London' 
WHEN org = 'spGermanyManagedService' THEN 'Europe/Berlin'
WHEN org = 'spDenmarkManagedService' THEN 'Europe/Copenhagen' WHEN org = 'cityOfMooneeValley' THEN 'Australia/Victoria' END AS timezone 
FROM `smartcloud.contravention_financials` 
  where date(time)>=date('2024-01-01') 
  and org = 'spDenmarkManagedService'
  --and transactionType = 'receipt'
  order by breachId
),
sites as (
select DISTINCT organization, region, site_id,site_name as name, timestamp

FROM `sc-neptune-production.managed_services_analytics.dim_site`
where organization = 'spDenmarkManagedService'
  --and site_id = @siteId AND region = @regionId 
  --_TABLE_SUFFIX = @orgId AND 
)
SELECT DISTINCT 
  p.ticketId, 
  p.offenseType, 
  p.feeAmount, 
  p.feeSource, 
  p.latestUpdateTime, 
  p.caseState, 
  p.caseSubState, 
  p.organization, 
  p.groupref, 
  s1.name,
  f.time,
FROM pcn p
left join finance f on
p.ticketId = f.breachId
INNER JOIN sites s1 ON 
  SPLIT(p.groupref, '#')[OFFSET(0)] = s1.region 
  AND SPLIT(p.groupref, '#')[OFFSET(1)] = s1.site_id
--where s1.name='Søndre Jernbanevej 28, 3400 Hillerød - (Plads 1017)'
and f.row_num = 1
ORDER BY p.ticketId
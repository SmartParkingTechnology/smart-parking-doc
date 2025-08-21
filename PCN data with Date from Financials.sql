with pcn as (
  select * from(
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
        ROW_NUMBER() OVER (PARTITION BY organization, g.name, breaches.ticketId ORDER BY fact.timestamp DESC) AS rn
    FROM 
        `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService`,
        UNNEST(breaches) AS breaches,        
        UNNEST(breaches.fees) AS fees,
        UNNEST(`groups`) AS g        
    WHERE 
        fees.offense.type NOT IN ('LateFee', 'CompensationFee') 
        AND fees.source = 'Smartcloud'
        AND state = 'Closed'
) AS subquery
WHERE rn = 1
),
finance as(
SELECT org,time,transactionType,breachId, description, debit, site,region,
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
select p.*, s.name, f.time,
coalesce(feeAmount,0) as Amount,
coalesce(debit,0) as Debit
 from pcn p
left join finance f on p.ticketId= f. breachId
inner join sites s ON p.organization = s.organization AND SPLIT(groupref, '#')[offset(0)]  = s.region AND SPLIT(groupref, '#')[offset(1)]  = s.site_id
--inner join sites s ON s.site_id = f.site
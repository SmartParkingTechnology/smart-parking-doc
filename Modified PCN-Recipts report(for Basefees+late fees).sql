WITH sites as (
select DISTINCT organization, region, site_id,site_name as name, timestamp

FROM `sc-neptune-production.managed_services_analytics.dim_site`
where organization = 'spDenmarkManagedService'
  and site_id = @siteId AND region = @regionId 
  --_TABLE_SUFFIX = @orgId AND 
),
pcn as (
SELECT * FROM (
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
       (SELECT SUM(t.amount) FROM UNNEST(breaches.transactions) t) AS transaction_amount,
       ROW_NUMBER() OVER (PARTITION BY organization, g.name,g.groupref, breaches.ticketId ORDER BY fact.timestamp DESC) AS rn
    FROM 
        `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService`,
    UNNEST(breaches) AS breaches,       
    UNNEST(breaches.fees) AS fees,     
    UNNEST(`groups`) AS g  
    --UNNEST (breaches.transactions) AS t # KIN MODIFIED
    WHERE 
        --fees.offense.type NOT IN ('LateFee', 'CompensationFee') 
        --AND fees.source = 'Smartcloud'
         state = 'Closed' 
         --and g.name like '%1017%'
         --and breaches.ticketId = "50000062"
) AS subquery
WHERE rn = 1
and organization = 'spDenmarkManagedService' 
AND DATE(latestUpdateTime) BETWEEN PARSE_DATE('%Y%m%d',@startDate) AND PARSE_DATE('%Y%m%d', @endDate)
)

select p.* , s.name, 
from pcn p
inner join sites s ON
p.organization = s.organization AND SPLIT(groupref, '#')[offset(0)]  = s.region AND SPLIT(groupref, '#')[offset(1)]  = s.site_id
where s.site_id = @siteId
and (p.caseSubState = 'PaidOnline' OR p.caseSubState = 'breach-paid-direct') # KIN MODIFIED
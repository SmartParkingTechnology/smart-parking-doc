WITH siteGroups AS (
    SELECT DISTINCT parent, groupId, name, p.LocationCode AS LocationCode
    FROM (
        SELECT parent, groupId, name
        FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
        WHERE STRUCT(key, timestamp) IN (
            SELECT STRUCT(key, MAX(timestamp))
            FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
            WHERE type = 'site'
            GROUP BY key
        )
        AND actionType != 'GroupDeleted'
    ) 
    INNER JOIN `sc-neptune-production.imperial3Sixty.pcn_details` p
    ON name = p.LocationName
    WHERE DATE(SAFE_CAST(PartitionedCurrentStateTime AS TIMESTAMP)) >= '2005-01-11'
),

breached_sites AS (
    SELECT DISTINCT s.name AS sitename, 
           DATE(b.issuingTime, timeZone) AS CaseDay,
           DATE(o.offenseTime,timeZone) as offenseTime,
           b.ticketId AS breachID,
           ROW_NUMBER() OVER (PARTITION BY ticketId ORDER BY o.offenseTime desc) as rn,
    FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spukscvs` cc
    LEFT JOIN UNNEST(breaches) AS b
    LEFT JOIN UNNEST(cc.groups) AS g
    LEFT JOIN UNNEST(b.parkingSession.offenses) as o 
    INNER JOIN siteGroups s ON g.groupRef = CONCAT(s.parent, '#', s.groupId)
    WHERE 
        
    DATE(o.offensetime, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
        --AND DATE(fact.timestamp, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
        --AND DATE(caseRef.timestamp, timeZone) BETWEEN PARSE_DATE('%Y%m%d',@DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
        AND g.type = 'site'
        --AND b.ticketId IS NOT NULL  -- Ensures breach exists
),

all_sites AS (
    SELECT DISTINCT name AS sitename,LocationCode
    FROM siteGroups
),

dates AS (
    SELECT DISTINCT date AS calender_date 
    FROM `sc-neptune-production.managed_services_analytics.dim_date` 
    WHERE date  BETWEEN PARSE_DATE('%Y%m%d',@DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
)
-- To filter out the sitename which have no breachID for selected date
SELECT a.sitename, LocationCode, d.calender_date, COALESCE(b.breachID, null) AS breachID, rn
FROM all_sites a
CROSS JOIN dates d
LEFT JOIN breached_sites b 
     ON a.sitename = b.sitename and d.calender_date = b.offenseTime
WHERE b.breachID  IS NULL
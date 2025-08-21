WITH sitegroup AS (
    SELECT DISTINCT 
        parent, 
        groupId, 
        name, 
        timestamp,
        JSON_EXTRACT_SCALAR(json, '$.metadata.threeSixtySiteCode') AS locationcode
    FROM `sc-neptune-production.group_actions.group_actions_spukscvs` 
    WHERE STRUCT(key, timestamp) IN (
        SELECT STRUCT(key, MAX(timestamp))
        FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
        WHERE type = 'site'
        GROUP BY key
    )
    AND actionType != 'GroupDeleted'
),
-- to bring the status of all the sites
status AS (
    SELECT * 
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY p.orgId, p.regionId, p.siteId ORDER BY p.createdTimestamp DESC) AS row_num 
        FROM `sc-neptune-production.smartcloud.site_contravention_status` p
    ) 
    WHERE row_num = 1
    and orgId = 'spukscvs'
),
-- To bring the Site with their respective status.
sitestatus as (
   SELECT a.*, b.status
   FROM sitegroup a
   LEFT JOIN status b
   ON a.groupId = b.siteId and a.parent = b.regionId
)
--All the sites which has Pcn issued and not issued along with their status.
SELECT groupRef,count(distinct breachID) as NumberOfBreaches, t.Name, t.LocationCode, t.status, State,
       DATE(offenseTime) as offenseDate,
       ARRAY_AGG (
                BreachID IGNORE NULLS
                 ) AS BreachIDs
FROM (
      SELECT * except(rn,rev,parent,groupId,name), 
             s.name as site, 
      from
    (
--To get the breachId for all the site along with the Offense date
      SELECT 
        ticketId as BreachID, rev, state, subState, 
        g.groupRef,
        DATETIME(o.offenseTime,timeZone) as offenseTime, 
        ROW_NUMBER() OVER (PARTITION BY ticketId ORDER BY fact.timestamp desc) as rn
      FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spukscvs` cc
        LEFT JOIN UNNEST(breaches) AS b
        LEFT JOIN UNNEST(cc.groups) AS g  
        LEFT JOIN UNNEST(b.parkingSession.offenses) as o 
      WHERE
        DATE(o.offenseTime, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
      AND 
        g.type = 'site'
) 
INNER JOIN sitegroup s ON
groupRef = CONCAT(s.parent, '#', s.groupId)
where rn = 1 AND NOT(state = "Closed" AND subState = "breachError") AND NOT(state = "Closed" AND subState = "nip") 

)
c
   right join sitestatus t 
  ON CONCAT(t.parent, '#', t.groupId) = c.groupRef 
group by c.groupRef,state, t.Name,offensedate,LocationCode, status
ORDER BY NumberOfBreaches ASC, t.Name;
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
) 
SELECT count(distinct breachID) as NumberOfBreaches,
breachID,
       notes,
       count(
       CASE 
       WHEN notes ='Initiate Foregin Workflow' THEN (BreachID) 
       END) as foreign_pcn, 
       externalbreachid as externalBreachId,
       s.Name, 
       State, 
       plate,
       DATE(caseCreatedTimestamp) as CaseDay,
       DATE(offenseTime) as offenseDate,
       groupId,
       ARRAY_AGG (
           BreachID IGNORE NULLS
                 ) AS All_BreachIDs,
-- distinct count of breachIDs excluding 'Foreign VRN'
       ARRAY_AGG(
         CASE
         WHEN notes NOT LIKE '%Foreign Workflow%'or notes is null and breachID is not null then 1
         END 
         IGNORE NULLS) AS BreachIds,

-- distinct count of all breachIDs 
         ARRAY_AGG(
         DISTINCT CASE
         WHEN notes LIKE '%Foreign Workflow%'or notes NOT LIKE '%Foreign Workflow%' or notes is null then 1
         END 
         IGNORE NULLS) AS Total_BreachIds,

-- distinct count of breachIDs which contain only 'Foreign VRN'
         ARRAY_AGG(
         CASE 
         WHEN notes like '%Foreign Workflow%' then 1
         ELSE NULL END 
         IGNORE NULLS) AS Foreign_VRN
 FROM (
      SELECT * except(rn,rev,parent,groupId,name), s.name as site 
      from
       (
       SELECT ticketId as BreachID, rev, state, subState, b.externalbreachid,b.parkingSession.vehicledetails.plate as plate,
              DATETIME(caseRef.timestamp, timeZone) as caseCreatedTimestamp,  -- when someone click "accept" in Process 2 to create a case
              DATETIME(b.issuingTime, timeZone) as issuingTime,   -- if workflow goes well, a breach document is created
              DATETIME(fact.timestamp, timeZone) as caseUpdateTimestamp,  --the process team may encounter issue with the document, they may close the case on the same day
              g.groupRef,
              DATETIME(o.offenseTime,timeZone) as offenseTime,
              n.text as notes,
              ROW_NUMBER() OVER (PARTITION BY ticketId ORDER BY fact.timestamp desc) as rn
       FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spukscvs` cc
       LEFT JOIN UNNEST(breaches) AS b
       LEFT JOIN UNNEST(cc.groups) AS g
       LEFT JOIN UNNEST(b.parkingSession.offenses) as o 
       LEFT JOIN UNNEST(notes) as n
       WHERE
       DATE(b.issuingTime, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) -- breach was issued on selected date
       AND
       DATE(fact.timestamp, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) -- the latest update comes from the same selected date
       AND
       DATE(caseRef.timestamp, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) -- avoid retrieving cases that were regenerated on this date
       AND
       g.type = 'site'
     ) 
     INNER JOIN siteGroups s ON
     groupRef = CONCAT(s.parent, '#', s.groupId)
     where rn = 1 AND NOT(state = "Closed" AND subState = "breachError") AND NOT(state = "Closed" AND subState = "nip") 

)
c
  RIGHT JOIN SiteGroups s 
  ON CONCAT(s.parent, '#', s.groupId) = c.groupRef 
  GROUP BY c.groupRef, s.Name, State, CaseDay, groupId,offensetime,externalbreachid,plate, notes, breachID
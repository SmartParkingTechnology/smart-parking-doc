WITH siteGroups  as (
select parent, groupId, name as GroupName
FROM `sc-neptune-production.group_actions.group_actions_scm`
where struct(key, timestamp) IN
(
select struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_scm`
where type = 'site'
group by key
)
AND actionType != 'GroupDeleted'
)

-- Main query
SELECT * 
EXCEPT (rn, factTime, caseid, parent, groupName, groupId, casetime, groupref)
FROM (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY CaseID, BreachID ORDER BY factTime DESC) AS rn
  FROM ((
    SELECT
      cc.id AS CaseID,
      b.ticketid AS BreachID,
      DATETIME_TRUNC(DATETIME(TIMESTAMP(fact.timestamp), timezone), SECOND) AS UpdateOrPaymentTime,
      DATETIME_TRUNC(DATETIME(b.issuingTime, timezone), SECOND) AS IssuingTime,
      DATETIME_TRUNC(DATETIME(cc.parkingSession.arrivalTime.timestamp, timezone), SECOND) AS arrival_timestamp,  -- Added arrival timestamp
      DATETIME_TRUNC(DATETIME(cc.parkingSession.departureTime.timestamp, timezone), SECOND) AS departure_timestamp,
      (SELECT SUM(f.amount) FROM UNNEST(breaches) b, UNNEST(b.fees) f) AS BreachValue,
      (SELECT SUM(t.amount) FROM UNNEST(breaches) b LEFT JOIN UNNEST(b.transactions) t) AS PCNReceipts,
      t.description AS paymentType,
      t.paymentReference AS paymentReference,
      cc.State AS State,
      cc.substate AS substate,
      DATETIME_TRUNC(DATETIME(cc.caseRef.timestamp, timezone), SECOND) AS CaseTime,
      DATETIME(fact.timestamp, timezone) AS factTime,
      g.groupref AS groupref,
      DATETIME(o.offenseTime, 'Pacific/Auckland') AS OffenseTime,
      --DATETIME(cc.parkingSession.arrivalTime.timestamp, 'Pacific/Auckland') AS ArrivalTime  -- Corrected timestamp selection
    FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_scm` cc
    LEFT JOIN UNNEST(cc.events) AS e
    LEFT JOIN UNNEST(cc.breaches) AS b
    LEFT JOIN UNNEST(b.transactions) AS t
    LEFT JOIN UNNEST(cc.GROUPS) AS g
    LEFT JOIN UNNEST(cc.parkingSession.offenses) AS o
  ) c
inner join SiteGroups s ON
c.groupRef = CONCAT(s.parent, '#', s.groupId)
))
where   
 DATE(caseTime) between PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) 
 and rn=1  and breachid is not null 

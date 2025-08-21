WITH siteGroups AS (
SELECT parent, groupId, name AS GroupName
FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
WHERE struct(key, timestamp) IN
(
SELECT struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
WHERE type = 'site'
GROUP BY key
)
AND actionType != 'GroupDeleted'
)

SELECT * FROM (
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY CaseID ORDER BY UpdateTime desc) AS rn,
ROW_NUMBER() OVER (PARTITION BY GroupName ORDER BY CaseTime) AS firstpcnforsite,

# there are multiple events of the same revision, will need to use the latest event
FROM ((
SELECT
  cc.id AS CaseID,
  CAST(rev AS INT64) AS Revision,
  State,
  CASE WHEN canceled.reason IS NULL THEN 'N/A' ELSE canceled.reason END AS CancelledReason,
  substate,
  DATETIME_TRUNC(DATETIME(caseRef.timestamp, timezone), SECOND) AS CaseTime,
  DATETIME_TRUNC(DATETIME(TIMESTAMP(e.timestamp), timezone), SECOND) AS UpdateTime,
  DATETIME_TRUNC(DATETIME(b.issuingTime, timezone), SECOND) AS IssuingTime,
  cc.parkingSession.vehicleDetails AS vehicleDetails,
  g.groupref,
  b.ticketid AS BreachID,
  DATETIME(o.offenseTime, 'Europe/Copenhagen') AS OffenseTime,
  o.policy AS BreachPolicy,
  CASE WHEN b.parkingSession.arrivaltime IS NOT NULL AND b.parkingSession.departuretime IS NOT NULL THEN 1 ELSE 0 END AS Stay,
  b.parkingSession.arrivaltime AS ArrivalTime,
  b.parkingSession.departuretime AS DepartureTime,
  CASE WHEN b.parkingSession.arrivaltime IS NOT NULL THEN 1 ELSE 0 END AS ArrivalCount,
  CASE WHEN b.parkingSession.departuretime IS NOT NULL THEN 1 ELSE 0 END AS DepartureCount,
  n.author.alias AS OfficerAlias,
  n.author.name AS OfficerName,
  
  (SELECT SUM(f.amount) FROM UNNEST(breaches) b, unnest(b.fees) f) AS BreachValue,
(SELECT SUM(t.amount) FROM UNNEST(breaches) b left join unnest(b.transactions) t) AS PCNReceipts
FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
LEFT JOIN UNNEST(events) AS e
LEFT JOIN UNNEST(breaches) AS b
LEFT JOIN UNNEST(cc.GROUPS) AS g
LEFT JOIN unnest(cc.parkingSession.offenses) o
LEFT JOIN UNNEST(notes) n
WHERE
  STRUCT(cc.id, CAST(cc.rev as INT64)) IN (
  SELECT STRUCT(id, MAX(CAST(rev AS INT64)))
  FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService`
  WHERE
    DATE(caseRef.timestamp, 'Europe/Copenhagen') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) AND cast(b.ticketId as INT64) >= 50000036
  GROUP BY id
) AND (state != "closed" AND subState != "breachError") AND (state != "closed" AND subState != "nip")
) c
INNER JOIN SiteGroups s ON
c.groupRef = CONCAT(s.parent, '#', s.groupId)
))
WHERE rn=1
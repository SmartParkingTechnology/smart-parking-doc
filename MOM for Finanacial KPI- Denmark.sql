WITH SiteGroups AS (
  SELECT parent, groupId, name AS GroupName
  FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
  WHERE STRUCT(key, timestamp) IN (
    SELECT STRUCT(key, MAX(timestamp))
    FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
    WHERE type = 'site'
    GROUP BY key
  )
  AND actionType != 'GroupDeleted'
),
MonthlyPCNCounts AS (
  SELECT 
    parent,
    GroupName,
    EXTRACT(YEAR FROM OffenseTime) AS year,
    EXTRACT(MONTH FROM OffenseTime) AS month_number,
    COUNT(DISTINCT BreachID) AS PCNCount,
    IFNULL(COUNT(DISTINCT CASE 
                          WHEN LOWER(substate) IN ('breach-paid-direct', 'bankpayment', 'paid-online', 'paid_online', 'paidonline') AND State = 'Closed' 
                          THEN BreachID 
                          END),0) AS PaidCount,
    COUNTIF(( BreachID is not null) AND substate IN ('nip', 'svc-cancel', 'breachError','lprError', 'noownershipdetails', 'updatedownershipdetails')) as CancelledPCNCount
  FROM (
    SELECT 
      cc.id AS CaseID,
      State,
      substate,
      DATETIME_TRUNC(DATETIME(caseRef.timestamp, timezone), SECOND) AS CaseTime,
      DATETIME(o.offenseTime, 'Europe/Copenhagen') AS OffenseTime,
      g.groupref,
      b.ticketid AS BreachID
    FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
    LEFT JOIN UNNEST(breaches) AS b
    LEFT JOIN UNNEST(cc.GROUPS) AS g
    LEFT JOIN UNNEST(cc.parkingSession.offenses) AS o
    WHERE g.type = 'site'
      AND DATE(OffenseTime) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
  ) AS c
  INNER JOIN SiteGroups s ON c.groupRef = CONCAT(s.parent, '#', s.groupId)
  GROUP BY parent, GroupName, year, month_number 
),
Result AS (
  SELECT *,
    LAG(PCNCount, 1) OVER (PARTITION BY GroupName ORDER BY year, month_number) AS previous_pcn_count,
    LAG(PaidCount, 1) OVER (PARTITION BY GroupName ORDER BY year, month_number) AS previous_paid_count,
    LAG(CancelledPCNCount, 1) OVER (PARTITION BY GroupName ORDER BY year, month_number) AS previous_cancelled_count
    
  FROM MonthlyPCNCounts
)

SELECT *
FROM Result
ORDER BY GroupName, year, month_number;

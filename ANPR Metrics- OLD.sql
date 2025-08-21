WITH siteGroups  AS (
SELECT 
   parent, 
   groupId, 
   name AS GroupName
FROM `sc-neptune-production.group_actions.group_actions_scm`
WHERE struct(key, timestamp) IN
(
   SELECT 
     struct(key, MAX(timestamp)) 
   FROM `sc-neptune-production.group_actions.group_actions_scm`
   WHERE type = 'site'
   GROUP BY key
)
AND actionType != 'GroupDeleted'
), 
contraventionDateDetails AS (
     SELECT * EXCEPT(rn,factTime,caseid)
      FROM (
          SELECT *, 
          ROW_NUMBER() OVER (PARTITION BY CaseID,breachid ORDER BY factTime desc) AS rn,
          FROM (
            (
             SELECT
               cc.id AS CaseID,
               b.ticketid AS BreachID,
               g.groupRef,
               DATETIME_TRUNC(DATETIME(TIMESTAMP(fact.timestamp), timezone), SECOND) AS UpdateOrPaymentTime,
               DATETIME_TRUNC(DATETIME(b.issuingTime, timezone), SECOND) AS IssuingTime,
               (SELECT SUM(f.amount) FROM UNNEST(breaches) b, unnest(b.fees) f) AS BreachValue,
               (SELECT SUM(t.amount) FROM UNNEST(breaches) b left join unnest(b.transactions) t) AS PCNReceipts,
               State,
               substate,
               DATETIME(fact.timestamp,timezone) AS factTime,
               DATETIME(o.offenseTime, timezone) AS OffenseTime
             FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_scm` cc
               LEFT JOIN UNNEST(breaches) AS b
               LEFT JOIN UNNEST(b.transactions) AS t
               LEFT JOIN UNNEST(cc.GROUPS) AS g
               LEFT JOIN unnest(cc.parkingSession.offenses) o
               WHERE g.type = 'site'
            )
            )
            )
      WHERE   
      DATE(OffenseTime) BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate) 
      AND rn=1 AND breachid IS NOT NULL
      --and substate not in ('nip', 'svc-cancel', 'breachError', 'lprError', 'noownershipdetails', 'updatedownershipdetails')
), 
processMatch AS (
  SELECT
    SAFE_DIVIDE(SUM(accepted), SUM(total)) AS processMatchRate,
    SUM(total) AS footFall,
    DATE(startProcessingTime, 'Pacific/Auckland') AS processingDate,
    org,
    region,
    site
  FROM `sc-neptune-production.smartcloud.process_metrics`
  WHERE DATE(startProcessingTime, 'Pacific/Auckland') BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate) and org = 'scm' 
  GROUP BY processingDate, org, region, site
) ,
rawMatch AS (
  SELECT 
    DATE(timestamp, 'Pacific/Auckland') AS rawMatchDate,
    SUM(CAST(entry AS INT64)) AS entries,
    SUM(IF(CAST(entry AS INT64) = 0, 1, 0)) AS exits,
    g.key AS gKey
  FROM `sc-neptune-production.lpr_events.lpr_events_scm`, UNNEST(`groups`) g
  WHERE DATE(timestamp, 'Pacific/Auckland') BETWEEN PARSE_DATE('%Y%m%d', @startDate) AND PARSE_DATE('%Y%m%d', @endDate)
  GROUP BY g.key, DATE(timestamp, 'Pacific/Auckland')
)


SELECT 
  org, 
  region, 
  site, 
  GroupName, 
  processMatchRate, 
  footFall, 
  processingDate, 
  ROUND(((entries+exits)/2),2) AS rawFootfall,
  IFNULL(count(BreachID),0) AS PCNCount, 
  IFNULL(SUM(IF(LOWER(substate) IN ('nip', 'svc-cancel', 'breachError'), 0, BreachValue)),0) AS PotentialPCNRevenue, 
  IFNULL(SUM(IF(LOWER(substate) IN ('defaulted') AND State = 'Closed', 0, PCNReceipts)),0) AS RealisedPCNRevenue, 
  IFNULL(COUNTIF(LOWER(substate) IN ('breach-paid-direct', 'bankpayment', 'paid-online', 'paid_online', 'paidonline') AND State = 'Closed'),0) AS PaidCount,
  COUNTIF((BreachID IS NOT NULL) AND substate IN ('nip', 'svc-cancel', 'breachError','lprError', 'noownershipdetails', 'updatedownershipdetails')) AS CancelledPCNCount, 
  COUNTIF(CONTAINS_SUBSTR(LOWER(substate), 'appeal') AND State = 'Closed') AS AppealsCount,
  COUNTIF(LOWER(substate) IN ('defaulted') AND State = 'Closed') AS DebtCount,
  SUM(IF(LOWER(substate) IN ('defaulted') AND State = 'Closed', PCNReceipts, 0)) AS DebtPaidReceipts
FROM siteGroups s
LEFT JOIN processMatch p ON s.parent = p.region AND s.groupId = p.site
LEFT JOIN rawMatch r ON p.processingDate = r.rawMatchDate AND gKey = CONCAT('scm', '#', region, '#', site)
LEFT JOIN contraventionDateDetails c ON c.groupRef = CONCAT(s.parent, '#', s.groupId) AND DATE(p.processingDate) = DATE(c.OffenseTime)
GROUP BY org, region, site, GroupName,processMatchRate, footFall, processingDate, entries, exits
ORDER BY processingDate

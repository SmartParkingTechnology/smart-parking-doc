WITH 
  sites AS (
    SELECT DISTINCT groupId, name, actionType, timestamp, organization, parent
    FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
    WHERE STRUCT(key, timestamp) IN (
      SELECT STRUCT(key, MAX(timestamp))
      FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
      WHERE type = 'site'
      GROUP BY key
    )
    AND actionType != 'GroupDeleted'
  ),
  siteStatus AS (
    SELECT DISTINCT orgId, regionId, siteId, status AS siteStatus, createdTimestamp AS statusTimestamp
    FROM `sc-neptune-production.smartcloud.site_contravention_status`
    WHERE STRUCT(siteId, createdTimestamp) IN (
      SELECT STRUCT(siteId, MAX(createdTimestamp))
      FROM `sc-neptune-production.smartcloud.site_contravention_status`
      GROUP BY regionId, siteId
    )
    AND orgId = 'spukscvs'
  ),
  siteGroups AS (
    SELECT s.organization, s.parent, s.groupId, s.name, t.statusTimestamp, t.siteStatus 
    FROM sites s
    LEFT JOIN siteStatus t
    ON s.parent = t.regionId AND s.groupId = t.siteId
    ORDER BY s.name
  ),
  contraventionDateDetails AS (
    SELECT * EXCEPT(rn, factTime, caseid)
    FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY CaseID, breachid ORDER BY factTime DESC) AS rn
      FROM (
        SELECT
          cc.id AS CaseID,
          b.ticketid AS BreachID,
          g.groupRef,
          DATETIME_TRUNC(DATETIME(TIMESTAMP(fact.timestamp), timezone), SECOND) AS UpdateOrPaymentTime,
          DATETIME_TRUNC(DATETIME(b.issuingTime, timezone), SECOND) AS IssuingTime,
          (SELECT SUM(f.amount) FROM UNNEST(breaches) b, UNNEST(b.fees) f) AS BreachValue,
          (SELECT SUM(t.amount) FROM UNNEST(breaches) b LEFT JOIN UNNEST(b.transactions) t) AS PCNReceipts,
          State,
          substate,
          DATETIME(fact.timestamp, timezone) AS factTime,
          DATETIME(o.offenseTime, timezone) AS OffenseTime
        FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spukscvs` cc
        LEFT JOIN UNNEST(breaches) AS b
        LEFT JOIN UNNEST(b.transactions) AS t
        LEFT JOIN UNNEST(cc.GROUPS) AS g
        LEFT JOIN UNNEST(cc.parkingSession.offenses) o
        WHERE g.type = 'site'
      )
    )
    WHERE DATE(OffenseTime) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
    AND rn = 1 AND breachid IS NOT NULL
  ),
  processMatch AS (
    SELECT
      SAFE_DIVIDE(SUM(accepted), SUM(total)) AS processMatchRate,
      SUM(total) AS footFall,
      DATE(startProcessingTime, 'Europe/London') AS processingDate,
      org,
      region,
      site
    FROM `sc-neptune-production.smartcloud.process_metrics`
    WHERE DATE(startProcessingTime, 'Europe/London') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
    AND org = 'spukscvs'
    GROUP BY processingDate, org, region, site
  ),
  rawMatch AS (
    SELECT 
      DATE(timestamp, 'Europe/London') AS rawMatchDate,
      SUM(CAST(entry AS INT64)) AS entries,
      SUM(IF(CAST(entry AS INT64) = 0, 1, 0)) AS exits,
      g.key AS gKey
    FROM `sc-neptune-production.lpr_events.lpr_events_spukscvs`, UNNEST(`groups`) g
    WHERE DATE(timestamp, 'Europe/London') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
    GROUP BY g.key, DATE(timestamp, 'Europe/London')
  )

SELECT 
  org, 
  p.region, 
  site, 
  s.name AS GroupName, 
  processMatchRate, 
  footFall, 
  processingDate, 
  ROUND(((entries + exits) / 2), 2) AS rawFootfall,
  CASE 
    WHEN siteStatus IS NULL THEN 'Active' 
    WHEN siteStatus = 'Created' THEN 'Pending' 
    ELSE siteStatus 
  END AS siteStatus,
  DATE(statusTimestamp, 'Europe/London') AS siteStatusDate,
  IFNULL(COUNT(BreachID), 0) AS PCNCount, 
  IFNULL(SUM(IF(LOWER(substate) IN ('nip', 'svc-cancel', 'breachError'), 0, BreachValue)), 0) AS PotentialPCNRevenue, 
  IFNULL(SUM(IF(LOWER(substate) IN ('defaulted') AND State = 'Closed', 0, PCNReceipts)), 0) AS RealisedPCNRevenue, 
  IFNULL(COUNTIF(LOWER(substate) IN ('breach-paid-direct', 'bankpayment', 'paid-online', 'paid_online', 'paidonline') AND State = 'Closed'), 0) AS PaidCount,
  COUNTIF((BreachID IS NOT NULL) AND substate IN ('nip', 'svc-cancel', 'breachError', 'lprError', 'noownershipdetails', 'updatedownershipdetails')) AS CancelledPCNCount, 
  COUNTIF(CONTAINS_SUBSTR(LOWER(substate), 'appeal') AND State = 'Closed') AS AppealsCount,
  COUNTIF(LOWER(substate) IN ('defaulted') AND State = 'Closed') AS DebtCount,
  SUM(IF(LOWER(substate) IN ('defaulted') AND State = 'Closed', PCNReceipts, 0)) AS DebtPaidReceipts
FROM siteGroups s
LEFT JOIN processMatch p 
  ON s.parent = p.region AND s.groupId = p.site
LEFT JOIN rawMatch r 
  ON p.processingDate = r.rawMatchDate AND gKey = CONCAT('spukscvs', '#', p.region, '#', site)
LEFT JOIN contraventionDateDetails c 
  ON c.groupRef = CONCAT(s.parent, '#', s.groupId) AND DATE(p.processingDate) = DATE(c.OffenseTime)
GROUP BY org, region, site, siteStatus, statusTimestamp, GroupName, processMatchRate, footFall, processingDate, entries, exits
ORDER BY processingDate;

## Testing for Finanacial KPI report for Denmark

WITH siteGroups as 
    (
      SELECT parent, groupId, name as GroupName
      FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
      WHERE struct(key, timestamp) IN
       (
         SELECT struct(key, max(timestamp)) FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
         WHERE type = 'site'
         GROUP BY key
       )
      AND actionType != 'GroupDeleted'
     )

       SELECT *,
         EXTRACT(YEAR FROM OffenseTime) AS year,
         EXTRACT(WEEK(MONDAY) FROM OffenseTime) AS week_number,
         --ROW_NUMBER() OVER (PARTITION BY BreachID ORDER BY OffenseTime) as rn,
         ROW_NUMBER() OVER (PARTITION BY GroupName ORDER BY OffenseTime asc) as firstpcnforsite,
         COUNT(DISTINCT BreachID) AS PCNCount,
         IFNULL(COUNTIF(lower(substate) IN ('breach-paid-direct', 'bankpayment', 'paid-online', 'paid_online', 'paidonline') AND State = 'Closed'),0) as PaidCount,
         COUNTIF(( BreachID is not null) AND substate IN ('nip', 'svc-cancel', 'breachError','lprError', 'noownershipdetails', 'updatedownershipdetails')) as CancelledPCNCount,
         IFNULL(SUM(IF(lower(substate) IN ('defaulted') AND State = 'Closed', 0, PCNReceipts)),0) as RealisedPCNRevenue,
         COUNTIF(CONTAINS_SUBSTR(LOWER(substate), 'appeal') AND State = 'Closed') as AppealsCount,
         --there are multiple events of the same revision, will need to use the latest event
       FROM (
                 SELECT
                   distinct cc.id AS CaseID,
                   State,
                   substate,
                   DATETIME(o.offenseTime, 'Europe/Copenhagen') as OffenseTime,
                   g.groupref,
                   b.ticketid AS BreachID,
                   (SELECT SUM(f.amount) FROM UNNEST(breaches) b, unnest(b.fees) f) AS BreachValue,
                   (SELECT SUM(t.amount) FROM UNNEST(breaches) b left join unnest(b.transactions) t) AS PCNReceipts,
                FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
                 
                 LEFT JOIN UNNEST(breaches) AS b
                 LEFT JOIN UNNEST(cc.GROUPS) AS g
                 LEFT JOIN unnest(cc.parkingSession.offenses) o
                 --LEFT JOIN UNNEST(notes) n
               WHERE
               g.type = 'site'
        and DATE(o.offenseTime, 'Europe/Copenhagen') between PARSE_DATE('%Y%m%d', '20250106') AND PARSE_DATE('%Y%m%d', '20250112') 
        
             ) c
                INNER JOIN SiteGroups s ON
                 c.groupRef = CONCAT(s.parent, '#', s.groupId)
                where groupId like '%1046%'and BreachID is not null
                and lower(substate) IN ('breach-paid-direct', 'bankpayment', 'paid-online', 'paid_online', 'paidonline') AND State = 'Closed'
  -- where DATE(OffenseTime) between PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
  
group by all


SELECT
  cc.id AS CaseID,
  CAST(rev AS INT64) AS Revision,
  State,
  substate,
  DATETIME_TRUNC(DATETIME(caseRef.timestamp, timezone), SECOND) AS CaseTime,
  DATETIME_TRUNC(DATETIME(TIMESTAMP(e.timestamp), timezone), SECOND) AS UpdateTime,
  DATETIME_TRUNC(DATETIME(b.issuingtime, timezone), SECOND) AS IssuingTime,
  cc.parkingSession.vehicleDetails.state as vehicleDetailsState,
  --To identify Foreign PCN 
  ARRAY_AGG(
         CASE 
         WHEN cc.parkingSession.vehicleDetails.state not in ('DK') then 1
         ELSE NULL END 
         IGNORE NULLS) AS Foreign_VRN,
  organization,
  (SELECT SUM(f.amount) FROM UNNEST(breaches) b, unnest(b.fees) f) AS PCNValue,
  (SELECT SUM(t.amount) FROM UNNEST(breaches) b left join unnest(b.transactions) t) AS PCNReceipts
FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
LEFT JOIN UNNEST(events) AS e
LEFT JOIN UNNEST(breaches) AS b
LEFT JOIN UNNEST(cc.GROUPS) AS g
LEFT JOIN UNNEST(notes) n
LEFT JOIN unnest(cc.parkingSession.offenses) o
WHERE
  STRUCT(cc.id, CAST(cc.rev as INT64)) IN 
         (
            SELECT STRUCT(id, MAX(CAST(rev as INT64)))
            FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService`
            WHERE DATE(caseRef.timestamp, 'Europe/Berlin') BETWEEN PARSE_DATE('%Y%m%d', '20240101') AND PARSE_DATE('%Y%m%d', '20250531')
            GROUP BY id
          ) 
 AND (state != "closed" AND subState != "breachError") AND (state != "closed" AND subState != "nip")
 And cc.parkingSession.vehicleDetails.state not in ('DK') 
group by all
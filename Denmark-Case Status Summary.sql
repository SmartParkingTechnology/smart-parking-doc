WITH latest_rev AS
(
SELECT 
DISTINCT
  cc.id AS CaseID,
  CAST(rev AS INT64) AS Revision,
  state,
  LOWER(state) AS State_LowerCase,
    CASE
    WHEN Substate IS NULL THEN Sub_States
    ELSE SubState
  END AS UpdatedSubStates,
  DATETIME_TRUNC(DATETIME(fact.timestamp,timezone),SECOND) AS UpdateTime,
  SPLIT(g.groupref, '#')[SAFE_OFFSET(1)] AS GroupName,
  b.ticketid AS BreachID,
  f.offense.type AS OffenseType,
  f.Amount AS Fee,
  ROW_NUMBER() OVER (PARTITION BY cc.id,b.ticketid ORDER BY fact.timestamp DESC) AS rn
FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService`` cc
  LEFT JOIN UNNEST(breaches) AS b
  LEFT JOIN UNNEST(b.fees) AS f
  LEFT JOIN UNNEST(subStates) as Sub_States
  LEFT JOIN UNNEST(cc.groups) AS g
  WHERE
  DATE(fact.timestamp,timezone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) AND
LOWER(g.type)='site'
AND cc.id NOT IN(
    SELECT DISTINCT cc.id
    FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
   LEFT JOIN UNNEST(subStates) AS Sub_States
WHERE LOWER(SubState) IN ('sysops','unprocessed','tech-cancel') 
   OR LOWER(Sub_States) IN ('sysops','unprocessed','tech-cancel')
   OR LOWER(canceled.reason) IN ('system admin task','removed for system testing or maintenance','cancelled for reload','case needs to be reloaded')
)
)
SELECT 
DISTINCT
CaseID,
Revision,
state,
State_LowerCase,
UpdateTime,
GroupName,
BreachID,
OffenseType,
Fee,
CASE 
    WHEN LOWER(UpdatedSubStates) IN ('breach_issued','breachissued','breach-issued') THEN 'BreachIssued'
    WHEN LOWER(UpdatedSubStates) IN ('reminder_issued','reminderissued','reminder-issued') THEN 'ReminderIssued'
    WHEN LOWER(UpdatedSubStates) IN ('noinformatiopresent','noinformationpresent') THEN 'NoInformationPresent'
    WHEN LOWER(UpdatedSubStates) IN ('paid','paid_online') THEN 'PaidOnline'
    WHEN LOWER(UpdatedSubStates) IN ('breach-paid-direct') THEN 'BreachPaidDirect'
    WHEN LOWER(UpdatedSubStates) IN ('appeal-accepted') THEN 'AppealAccepted'
    WHEN LOWER(UpdatedSubStates) IN ('nodetail') THEN 'NoDetail'
    WHEN LOWER(UpdatedSubStates) IN ('defaulted') THEN 'Defaulted'
    WHEN LOWER(UpdatedSubStates) IN ('special-circumstance') THEN 'SpecialCircumstance'
    ELSE UpdatedSubStates
END AS UpdatedSubState,
SiteName
FROM latest_rev

--INNER JOIN to retrieve site name
INNER JOIN 
(
WITH latest_groupID AS (
SELECT DISTINCT name, groupId,
ROW_NUMBER() OVER (PARTITION BY groupId ORDER BY timestamp DESC) AS grn
FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService` )
 SELECT DISTINCT name AS SiteName,groupID  FROM latest_groupID
WHERE grn=1
 )
 
 ON 
GroupName=groupID
WHERE rn=1
AND 
UpdatedSubStates IS NOT NULL
--AND Updatedsubstates= 'appeal-pending'
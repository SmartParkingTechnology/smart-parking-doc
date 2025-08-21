WITH latest_rev AS (
  SELECT 
    DISTINCT
    cc.id AS CaseID,
    CAST(rev AS INT64) AS Revision,
    state,
    substate,
    LOWER(state) AS State_LowerCase,
    CASE
      WHEN Substate IS NULL THEN Sub_States
      ELSE SubState
    END AS UpdatedSubStates,
    DATETIME_TRUNC(DATETIME(fact.timestamp, timezone), SECOND) AS UpdateTime,
    DATETIME(o.offenseTime, 'Europe/Copenhagen') AS OffenseTime,
    SPLIT(g.groupref, '#')[SAFE_OFFSET(1)] AS GroupName,
    b.ticketid AS BreachID,
    f.offense.type AS OffenseType,
    f.Amount AS Fee,
    ROW_NUMBER() OVER (PARTITION BY cc.id, b.ticketid ORDER BY fact.timestamp DESC) AS rn
  FROM
    `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
    LEFT JOIN UNNEST(breaches) AS b
    LEFT JOIN UNNEST(b.fees) AS f
    LEFT JOIN UNNEST(subStates) AS Sub_States
    LEFT JOIN UNNEST(cc.groups) AS g
    LEFT JOIN UNNEST(cc.parkingSession.offenses) AS o
  WHERE
    LOWER(g.type) = 'site'
    AND cc.id NOT IN (
      SELECT DISTINCT cc.id
      FROM
        `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spDenmarkManagedService` cc
        LEFT JOIN UNNEST(subStates) AS Sub_States
      WHERE LOWER(SubState) IN ('sysops', 'unprocessed', 'tech-cancel') 
        OR LOWER(Sub_States) IN ('sysops', 'unprocessed', 'tech-cancel')
        OR LOWER(canceled.reason) IN ('system admin task', 'removed for system testing or maintenance', 'cancelled for reload', 'case needs to be reloaded')
    )
),
ranked_data AS (
  SELECT 
    DISTINCT
    GroupName,
    CaseID,
    BreachID,
    OffenseTime,
    Revision,
    OffenseType,
    state,
    substate,
    State_LowerCase,
    UpdateTime,
    Fee,
    rn,
    ROW_NUMBER() OVER (PARTITION BY CaseID, BreachID, OffenseTime ORDER BY UpdateTime DESC) AS row_num,
    CASE 
      WHEN LOWER(UpdatedSubStates) IN ('breach_issued', 'breachissued', 'breach-issued') THEN 'BreachIssued'
      WHEN LOWER(UpdatedSubStates) IN ('reminder_issued', 'reminderissued', 'reminder-issued') THEN 'ReminderIssued'
      WHEN LOWER(UpdatedSubStates) IN ('noinformatiopresent', 'noinformationpresent') THEN 'NoInformationPresent'
      WHEN LOWER(UpdatedSubStates) IN ('paid', 'paid_online') THEN 'PaidOnline'
      WHEN LOWER(UpdatedSubStates) IN ('breach-paid-direct') THEN 'BreachPaidDirect'
      WHEN LOWER(UpdatedSubStates) IN ('appeal-accepted') THEN 'AppealAccepted'
      WHEN LOWER(UpdatedSubStates) IN ('nodetail') THEN 'NoDetail'
      WHEN LOWER(UpdatedSubStates) IN ('defaulted') THEN 'Defaulted'
      WHEN LOWER(UpdatedSubStates) IN ('special-circumstance') THEN 'SpecialCircumstance'
      ELSE UpdatedSubStates
    END AS UpdatedSubState
  FROM latest_rev
),
filtered_data AS (
  SELECT *
  FROM ranked_data
  WHERE row_num = 1 -- Ensure only the latest revision
)
SELECT *
FROM filtered_data
WHERE DATE(UpdateTime) BETWEEN PARSE_DATE('%Y%m%d', '20240701') AND PARSE_DATE('%Y%m%d', '20241031')

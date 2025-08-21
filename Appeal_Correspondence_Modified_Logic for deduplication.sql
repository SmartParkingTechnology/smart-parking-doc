WITH ordered_events AS (
  SELECT
    caseUri,
    DATE(caseEvent.timestamp, 'Pacific/Auckland') AS created_date,
    JSON_VALUE(caseEvent.data, '$.value') AS status,
    LAG(JSON_VALUE(caseEvent.data, '$.value')) OVER (PARTITION BY caseUri ORDER BY caseEvent.timestamp) AS prev_status
  FROM `sc-neptune-production.contravention_user_data.user_case_events_scm`
  WHERE DATE(caseEvent.timestamp, 'Pacific/Auckland') 
          BETWEEN PARSE_DATE('%Y%m%d', '20250201')
          --AND PARSE_DATE('%Y%m%d', '20250330')
          AND CURRENT_DATE('Pacific/Auckland')  -- or use a dynamic upper bound
          AND caseEvent.type = 'Tag' 
   -- AND caseUri = 'sc://scm/contraventions/parking/c#Parking#16r1pi#1742499647377'
)
, transitions AS (
  SELECT
    created_date,
    -- Count only when status changes or first row (prev_status IS NULL)
   SUM( CASE 
      WHEN status = 'Unresolved' AND (prev_status IS DISTINCT FROM 'Unresolved' OR prev_status IS NULL) THEN 1 ELSE 0 END )AS unresolved_case_count,
   SUM( CASE 
      WHEN status = 'Resolved' AND (prev_status IS DISTINCT FROM 'Resolved' OR prev_status IS NULL) THEN 1 ELSE 0 END) AS resolved_case_count
      
  FROM ordered_events
  group by created_date
)
SELECT
created_date,
   unresolved_case_count,
  resolved_case_count,
  SUM(unresolved_case_count - resolved_case_count) OVER (
            ORDER BY created_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_unresolved
FROM transitions
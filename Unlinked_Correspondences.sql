with transitions AS (
SELECT *,
  ROW_NUMBER() OVER (PARTITION BY uri, transition_type ORDER BY date asc) as rn
FROM(
  SELECT 
    DATE(timestamp, 'Pacific/Auckland') AS date,
    uri,
    JSON_EXTRACT(data, '$.mailUpdate.labels') as labels,
    JSON_VALUE(userJson, '$.name') AS user_name,
    CASE
          WHEN JSON_EXTRACT_SCALAR(data, '$.mailUpdate.labels.Resolved') = 'false'
           AND JSON_EXTRACT_SCALAR(data, '$.mailUpdate.labels.Unresolved') = 'true'
        THEN 'Unresolved'

      WHEN JSON_EXTRACT_SCALAR(data, '$.mailUpdate.labels.Resolved') = 'true'
           AND JSON_EXTRACT_SCALAR(data, '$.mailUpdate.labels.Unresolved') = 'false'
        THEN 'Resolved'
      
      WHEN JSON_EXTRACT_SCALAR(data, '$.mailUpdate.labels.Unresolved') = 'true'
        THEN 'Unresolved'
      ELSE 'Other'
    END AS transition_type,
    FROM `sc-neptune-production.smartcloud.correspondence_action_scm`
    WHERE DATE(timestamp, 'Pacific/Auckland') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
    AND JSON_EXTRACT(data, '$.mailUpdate.labels') IS NOT NULL
)
),

-- Count transitions per day and uri
daily_counts AS (
  SELECT 
    date,
    uri,
    user_name,
    transition_type,
    COUNT(*) AS count,
    rn
  FROM transitions
  WHERE transition_type IN ('Unresolved', 'Resolved')
  AND rn=1
  GROUP BY date, uri, user_name, transition_type, rn
)

-- Pivot to separate columns for each transition type

  SELECT 
    date,
    uri,
    user_name,
    rn,
    transition_type,
    IFNULL(SUM(CASE WHEN transition_type = 'Unresolved' THEN count END), 0) AS unresolved_count,
    IFNULL(SUM(CASE WHEN transition_type = 'Resolved' THEN count END), 0) AS resolved_count,
    --IFNULL(SUM(CASE WHEN transition_type = 'Unresolved' THEN count END), 0) AS u,
  FROM daily_counts d
  GROUP BY date, uri,user_name, rn,transition_type


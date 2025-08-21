-- Breach Report Pivoted by Weekday: This Week vs Last Week (Dynamic)

WITH siteGroups AS (
  SELECT parent, groupId, name
  FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
  WHERE STRUCT(key, timestamp) IN (
    SELECT STRUCT(key, MAX(timestamp))
    FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
    WHERE type = 'site'
    GROUP BY key
  )
  AND actionType != 'GroupDeleted'
),

breach_data AS (
  SELECT 
    CONCAT(s.parent, '#', s.groupId) AS group_ref,
    s.name AS site_name,
    DATE(o.offenseTime, timeZone) AS offense_date,
    FORMAT_DATE('%A', DATE(o.offenseTime, timeZone)) AS weekday_name,
    EXTRACT(DAYOFWEEK FROM DATE(o.offenseTime, timeZone)) AS day_number,
    ticketId AS breach_id,
    CASE
      WHEN DATE(o.offenseTime, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_END_DATE) - INTERVAL 13 DAY 
                                           AND PARSE_DATE('%Y%m%d', @DS_END_DATE)- INTERVAL 7 DAY
        THEN 'This Week'
      WHEN DATE(o.offenseTime, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_END_DATE) - INTERVAL 21 DAY 
                                           AND PARSE_DATE('%Y%m%d', @DS_END_DATE) - INTERVAL 13 DAY
        THEN 'Last Week'
      ELSE NULL
    END AS week_label
  FROM `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_spukscvs` cc
  LEFT JOIN UNNEST(breaches) AS b
  LEFT JOIN UNNEST(cc.groups) AS g
  LEFT JOIN UNNEST(b.parkingSession.offenses) AS o
  LEFT JOIN siteGroups s ON CONCAT(s.parent, '#', s.groupId) = g.groupRef
  WHERE
    g.type = 'site'
    AND DATE(o.offenseTime, timeZone) BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) - INTERVAL 13 DAY 
                                          AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
),
-- Generate all day labels
day_labels AS (
  SELECT 'Monday' AS day_label UNION ALL
  SELECT 'Last Monday' UNION ALL
  SELECT 'Tuesday' UNION ALL
  SELECT 'Last Tuesday' UNION ALL
  SELECT 'Wednesday' UNION ALL
  SELECT 'Last Wednesday' UNION ALL
  SELECT 'Thursday' UNION ALL
  SELECT 'Last Thursday' UNION ALL
  SELECT 'Friday' UNION ALL
  SELECT 'Last Friday' UNION ALL
  SELECT 'Saturday' UNION ALL
  SELECT 'Last Saturday' UNION ALL
  SELECT 'Sunday' UNION ALL
  SELECT 'Last Sunday'
),
-- Your existing breach data from earlier
breach_counts AS (
  SELECT
    site_name,
    CONCAT(
      CASE WHEN week_label = 'Last Week' THEN 'Last ' ELSE '' END,
      weekday_name
    ) AS day_label,
    COUNT(DISTINCT breach_id) AS breach_count
  FROM breach_data
  WHERE week_label IS NOT NULL
  GROUP BY site_name, day_label
),
-- CROSS JOIN to ensure all site-day-label combinations
all_site_labels AS (
  SELECT DISTINCT site_name FROM breach_counts
),
grid AS (
  SELECT s.site_name, d.day_label
  FROM all_site_labels s
  CROSS JOIN day_labels d
)
-- Final result: left join actual data onto full grid
SELECT 
  g.site_name,
  g.day_label,
  COALESCE(b.breach_count, 0) AS breach_count
FROM grid g
LEFT JOIN breach_counts b
  ON g.site_name = b.site_name AND g.day_label = b.day_label
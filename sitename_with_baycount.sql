SELECT DISTINCT groupId, name,
  JSON_VALUE(JSON_EXTRACT(json, '$.metadata.bayCount')) AS bayCount
  FROM `sc-neptune-production.group_actions.group_actions_scm`
  WHERE STRUCT(key, timestamp) IN (
    SELECT STRUCT(key, MAX(timestamp)) 
    FROM `sc-neptune-production.group_actions.group_actions_scm`
    WHERE type = 'site'
    GROUP BY key
  )
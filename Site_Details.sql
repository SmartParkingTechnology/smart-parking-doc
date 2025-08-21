SELECT 
  organization AS orgID,
  parent AS region,
  groupId AS site_id,
  name AS SiteName,
  TRIM(tag) AS tag,
  timeZone
FROM `sc-neptune-production.group_actions.group_actions_scm`,
UNNEST(
  SPLIT(REGEXP_REPLACE(JSON_VALUE(json, '$.metadata.tags'), r'[\[\]"]', ''), ',')
) AS tag
WHERE STRUCT(KEY, timestamp) IN (
  SELECT STRUCT(KEY, MAX(timestamp))
  FROM `sc-neptune-production.group_actions.group_actions_scm`
  WHERE type = 'site'
  GROUP BY KEY
)
AND actionType != 'GroupDeleted'

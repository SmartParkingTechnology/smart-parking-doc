WITH geomap_coordinates AS (
  SELECT 
    geometry, 
    ST_CENTROID(geometry) AS centroid,
    region,
    names.primary AS name
  FROM
    bigquery-public-data.overture_maps.division_area
  --WHERE region LIKE 'NZ%'
   --region LIKE 'NZ%' OR 
    --region LIKE 'GB%' OR 
    --region LIKE 'DE%' OR 
    --region LIKE 'DK%' OR 
    --region LIKE 'CH%' OR 
    region = 'US-FL'-- OR 
    --region = 'US-GA' OR 
    --region = 'US-TX'
    and subtype = 'locality'
--AND subtype = 'locality'
),
siteDetails AS (
  SELECT 
    parent, 
    groupId, 
    name AS SiteName, 
    description,
    JSON_EXTRACT(json, '$.metadata.industries') AS industries_type,
    ST_GEOGPOINT(
      CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT_SCALAR(json, '$.metadata.location'), '$.longitude')) AS FLOAT64),
      CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(JSON_EXTRACT_SCALAR(json, '$.metadata.location'), '$.latitude')) AS FLOAT64)
    ) AS location
  FROM
    `sc-neptune-production.group_actions.group_actions*`
  WHERE STRUCT(KEY, timestamp) IN (
        SELECT STRUCT(KEY, MAX(timestamp))
        FROM `sc-neptune-production.group_actions.group_actions*`
        WHERE type = 'site'
        GROUP BY KEY
  )
  AND actionType != 'GroupDeleted'
),
status AS (
    SELECT * 
    FROM (
        SELECT * except(status),
              CASE
              WHEN status = 'Active' THEN 'Active'
              WHEN status = 'Created' THEN 'Created'
              WHEN status = 'Stopped' THEN 'Stopped' 
              WHEN status IS null THEN 'Active' 
              END AS site_status,

               ROW_NUMBER() OVER (PARTITION BY p.orgId, p.regionId, p.siteId ORDER BY p.createdTimestamp DESC) AS row_num 
        FROM `sc-neptune-production.smartcloud.site_contravention_status` p
    ) 
    WHERE row_num = 1
    --AND orgId = 'scm'
)
      
SELECT c.* , COALESCE(b.site_status, 'Active') AS site_status, FROM 
          (
            SELECT 
               g.geometry,
               g.centroid,
               g.name,
               s.location,
               g.region,
               s.groupId,
               s.parent,
               s.SiteName,
               s.industries_type
            FROM geomap_coordinates g
            LEFT JOIN siteDetails s
            ON ST_CONTAINS(g.geometry, s.location)
        )c
     LEFT JOIN status b
     ON c.groupId = b.siteId and c.parent = b.regionId
  

WITH sitebay AS (
    SELECT DISTINCT 
        organization AS orgID, 
        parent AS region, 
        groupId AS site_id,  
        name AS SiteName,
        SAFE_CAST(JSON_VALUE(JSON_EXTRACT(json, '$.metadata.bayCount')) AS INT64) AS bay_count,
        timezone
    FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
    WHERE STRUCT (KEY, timestamp) IN (
        SELECT STRUCT (KEY, MAX(timestamp))
        FROM `sc-neptune-production.group_actions.group_actions_spukscvs`
        WHERE type = 'site'
        GROUP BY KEY 
    )
    AND actiontype <> 'GroupDeleted'
),
time_intervals AS (
    SELECT 
        s.site_id,
        s.SiteName,
        COALESCE(s.bay_count, 0) AS bay_count,
        s.timezone,
        timestamp_value AS time
    FROM sitebay s
    CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(
        TIMESTAMP('2025-03-02 00:00:00', s.timezone),  
        TIMESTAMP('2025-03-02 23:59:59', s.timezone),  
        INTERVAL 15 MINUTE
    )) AS timestamp_value
)
SELECT *
FROM time_intervals
ORDER BY SiteName, time;
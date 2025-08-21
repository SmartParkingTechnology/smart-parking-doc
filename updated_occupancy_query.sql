WITH sitebay AS (
    SELECT DISTINCT 
        organization AS orgID, 
        parent AS region, 
        groupId AS site_id,  
        name AS SiteName,
        SAFE_CAST(JSON_VALUE(JSON_EXTRACT(json, '$.metadata.bayCount')) AS INT64) AS bay_count,
        timezone
    FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
    WHERE STRUCT (KEY, timestamp) IN (
        SELECT STRUCT (KEY, MAX(timestamp))
        FROM `sc-neptune-production.group_actions.group_actions_spDenmarkManagedService`
        WHERE type = 'site'
        GROUP BY KEY 
    )
    AND actiontype <> 'GroupDeleted'
    and groupId = @siteId
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
        TIMESTAMP(CONCAT(FORMAT_DATE('%Y-%m-%d', DATE_SUB(PARSE_DATE('%Y%m%d', @endDate), INTERVAL 6 DAY)), ' 00:00:00'), s.timezone),
        --TIMESTAMP(CONCAT(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', @startDate)), ' 00:00:00'), s.timezone),
        TIMESTAMP(CONCAT(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', @endDate)), ' 23:59:59'), s.timezone),
        INTERVAL 15 MINUTE
    )) AS timestamp_value
),
base_data AS (
   Select * from (
             SELECT DISTINCT
                 SPLIT(orsId, '#')[SAFE_OFFSET(0)] AS org_id,
                 SPLIT(orsId, '#')[SAFE_OFFSET(1)] AS region_id,
                 SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS site_id,
                 inPlate AS plate,
                 arrivalTime,
                 departureTime,
        
            FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
           )
    WHERE 
       DATE(arrivalTime,'Europe/Copenhagen') BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d', @endDate), INTERVAL 6 DAY) AND (PARSE_DATE('%Y%m%d',@endDate))
       and org_id = @orgId AND region_id = @regionId and site_id = @siteId 
),
aggregated_data AS (
    select site_id,time_intervals,
        COUNTIF(direction = 'entry') AS entryCount,
        COUNTIF(direction = 'exit') AS exitCount from
        (
    SELECT DISTINCT
        site_id, plate,
        timestamp(DATETIME(TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(arrivalTime)), 15 * 60) * 15 * 60), 'Europe/Copenhagen')) AS time_intervals, 
        'entry' AS direction
    FROM base_data
    WHERE arrivalTime IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        site_id, plate,
        timestamp(DATETIME(TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(departureTime)), 15 * 60) * 15 * 60), 'Europe/Copenhagen')) AS time_intervals, 
        'exit' AS direction
    FROM base_data 
    WHERE departureTime IS NOT NULL
        )
        GROUP BY site_id,time_intervals
),
merged_data as(
    SELECT distinct
        ti.SiteName,
        ti.bay_count,
        ti.time,
        DATE(ti.time) AS date,
        FORMAT_TIME('%H:%M', TIME(time)) AS time_slot,
        COALESCE(ad.entryCount, 0) AS entryCount,
        COALESCE(ad.exitCount, 0) AS exitCount
    FROM time_intervals ti
    LEFT JOIN aggregated_data ad 
           ON ti.time = ad.time_intervals
           AND ti.site_id = ad.site_id
         --where ti.site_id like '%plads1017SndreJernbanevej283400HIllerd%'
      
)
SELECT distinct
    SiteName,
    bay_count,
    --Date, 
    time,
    time_slot,
    entryCount,
    exitCount,
    SUM(entryCount - exitCount) OVER (PARTITION BY date ORDER BY time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS currentOccupancy
FROM merged_data
ORDER BY  time ASC;


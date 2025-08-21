WITH sitebay AS (
    SELECT DISTINCT 
        organization AS orgID, 
        parent AS region, 
        groupId AS site_id,  
        name AS SiteName,
        SAFE_CAST(JSON_VALUE(JSON_EXTRACT(json, '$.metadata.bayCount')) AS INT64) AS bay_count,
        timezone
    FROM `sc-neptune-production.group_actions.group_actions_scm`
    WHERE STRUCT (KEY, timestamp) IN (
        SELECT STRUCT (KEY, MAX(timestamp))
        FROM `sc-neptune-production.group_actions.group_actions_scm`
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
       TIMESTAMP(DATETIME(timestamp_value,s.timezone)) AS time
     FROM sitebay s
        CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(
        TIMESTAMP(CONCAT(FORMAT_DATE('%Y-%m-%d', (PARSE_DATE('%Y%m%d', @ds_start_date))), ' 00:00:00'), s.timezone),
        TIMESTAMP(CONCAT(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d',@ds_end_date)), ' 23:59:59'), s.timezone),
        INTERVAL 15 MINUTE
    )) AS timestamp_value
),
base_data AS (
  
    SELECT DISTINCT
                 SPLIT(orsId, '#')[SAFE_OFFSET(0)] AS org_id,
                 SPLIT(orsId, '#')[SAFE_OFFSET(1)] AS region_id,
                 SPLIT(orsId, '#')[SAFE_OFFSET(2)] AS site_id,
                 inPlate AS plate,
                 arrivalTime,
                 datetime(arrivalTime,'Pacific/Auckland') as arrivalDate,
                 departureTime,
                 datetime(departureTime,'Pacific/Auckland') as departureDate
        
       FROM `sc-neptune-production.smartcloud.lpr_matching_plates`
       WHERE 
       DATE(arrivalTime,'Pacific/Auckland') BETWEEN 
       date_sub(DATE(timestamp(PARSE_DATE('%Y%m%d', @ds_start_date)),'Pacific/Auckland'), interval 1 day) AND DATE(timestamp(PARSE_DATE('%Y%m%d',@ds_end_date)),'Pacific/Auckland')
 ),
aggregated_data AS (
    select site_id,time_intervals,
        COUNTIF(direction = 'entry') AS entryCount,
        COUNTIF(direction = 'exit') AS exitCount from
        (
    SELECT DISTINCT
        site_id, plate,
        timestamp(DATETIME(TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(arrivalTime)), 15 * 60) * 15 * 60), 'Pacific/Auckland')) AS time_intervals, 
        'entry' AS direction
    FROM base_data
    WHERE arrivalDate IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        site_id, plate,
        timestamp(DATETIME(TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(departureTime)), 15 * 60) * 15 * 60), 'Pacific/Auckland')) AS time_intervals, 
        'exit' AS direction
    FROM base_data 
    WHERE departureDate IS NOT NULL
        )
        GROUP BY site_id,time_intervals
),
merged_data as(
    SELECT distinct
        ti.site_id,
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
        
      
),
pre_existing_occupancy AS (
  SELECT 
    site_id,
    SUM(entryCount - exitCount) AS base_occupancy
  FROM (
    SELECT 
      site_id,
      timestamp(DATETIME(TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(arrivalTime)), 15 * 60) * 15 * 60), 'Pacific/Auckland')) AS time_slot,
      1 AS entryCount,
      0 AS exitCount
    FROM base_data
    WHERE arrivalDate IS NOT NULL

    UNION ALL

    SELECT 
      site_id,
      timestamp(DATETIME(TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(departureTime)), 15 * 60) * 15 * 60), 'Pacific/Auckland')) AS time_slot,
      0 AS entryCount,
      1 AS exitCount
    FROM base_data
    WHERE departureDate IS NOT NULL
  )
 WHERE time_slot <= TIMESTAMP(CONCAT(FORMAT_DATE('%Y-%m-%d', DATE_SUB(PARSE_DATE('%Y%m%d', @ds_start_date), INTERVAL 1 DAY)), ' 23:45:00'), 'Pacific/Auckland')

  GROUP BY site_id
)


SELECT distinct
    md.SiteName,
    md.bay_count,    
    md.Date, 
    md.time,
    md.time_slot,
    md.entryCount,
    md.exitCount,
    COALESCE(p.base_occupancy, 0) as pre_occupancy,
    COALESCE(p.base_occupancy, 0) --as pre_occupancy
    +
    SUM(entryCount - exitCount) OVER (PARTITION BY SiteName ORDER BY time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS currentOccupancy
FROM merged_data md
left join pre_existing_occupancy p
on md.site_id = p.site_id
ORDER BY  time ASC;

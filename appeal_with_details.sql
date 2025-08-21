WITH base_data AS (
    SELECT DISTINCT
        caseUri,
        caseEvent.data, 
        caseEvent.type, 
        caseEvent.author.name AS author_name,
        DATETIME(caseRef.timestamp, 'Pacific/Auckland') AS created_timestamp,
        DATETIME(caseEvent.timestamp, 'Pacific/Auckland') AS updated_timestamp,
        COALESCE(
            JSON_VALUE(caseEvent.data, '$.tags.correspondence'),
            JSON_VALUE(caseEvent.data, '$.value') 
        ) AS status
    FROM `sc-neptune-production.contravention_user_data.user_case_events_scm`
    WHERE DATE(caseRef.timestamp, 'Pacific/Auckland') >= PARSE_DATE('%Y%m%d', '20250101')
--BETWEEN PARSE_DATE('%Y%m%d',@DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
        AND caseEvent.type = 'Tag'
        --AND caseUri LIKE '%1741205107351%'
),
status_change AS (
    SELECT 
        caseUri,
        status,
        created_timestamp,
        updated_timestamp,
        author_name,
        LEAD(status) OVER (PARTITION BY caseUri ORDER BY updated_timestamp) AS next_status,
        LEAD(updated_timestamp) OVER (PARTITION BY caseUri ORDER BY updated_timestamp) AS next_status_timestamp,
        LEAD(author_name) OVER (PARTITION BY caseUri ORDER BY updated_timestamp) AS next_author_name,
        DATETIME_DIFF(
            LEAD(updated_timestamp) OVER (PARTITION BY caseUri ORDER BY updated_timestamp), 
            updated_timestamp, 
            DAY
        ) AS duration_days, --the difference between updated date and next date along with the historical date.. all the transitional data.
        DATETIME_DIFF(
            LEAD(updated_timestamp) OVER (PARTITION BY caseUri ORDER BY updated_timestamp), 
            updated_timestamp, 
           HOUR
        ) AS duration_hours
    FROM base_data
),
first_unresolved AS (
    SELECT 
        caseUri,
        status,
        created_timestamp,
        updated_timestamp,
        author_name,
        next_status,
        next_status_timestamp,
        next_author_name,
        -- Get the FIRST author_name where status changes from 'Unresolved' -> 'Resolved'
        FIRST_VALUE(next_author_name IGNORE NULLS) OVER (PARTITION BY caseUri ORDER BY updated_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS first_resolver,
        duration_days,
        duration_hours
    FROM status_change
),
breaches AS (
     SELECT distinct
            id AS caseId,
            breaches.ticketId AS breachId, 
            uri,
            DATETIME(o.offenseTime, 'Pacific/Auckland') AS OffenseTime,
            DATETIME(fact.timestamp, 'Pacific/Auckland') AS latestUpdateTime,  
            --state AS caseState,
            --subState AS caseSubState,
            --organization,
            --g.name AS Site,
            --g.groupref,
        ROW_NUMBER() OVER (PARTITION BY breaches.ticketId ORDER BY fact.timestamp DESC) AS row_num
    FROM 
        `sc-neptune-production.contravention_parking_cases_v2.contravention_parking_cases_v2_scm` cc,
        UNNEST(breaches) AS breaches,        
        UNNEST(breaches.fees) AS fees,
        UNNEST(`groups`) AS g,
        UNNEST(cc.parkingSession.offenses) AS o
    WHERE DATE(fact.timestamp,'Pacific/Auckland') >= PARSE_DATE('%Y%m%d', '20230101'))
--BETWEEN PARSE_DATE('%Y%m%d', @ds_start_date) AND PARSE_DATE('%Y%m%d', @ds_end_date))

SELECT 
    b.caseId,
    b.breachId,
    caseUri, 
    status,
    created_timestamp, 
    updated_timestamp, 
    next_status, 
    next_status_timestamp,
    -- Assign the resolver to the first "Unresolved" status
    CASE 
        WHEN status = 'Unresolved' THEN first_resolver 
        ELSE author_name 
    END AS final_author_name,
   duration_days,
   duration_hours
FROM first_unresolved f
LEFT JOIN breaches b
ON f.caseUri = b. uri 
WHERE (status = 'Unresolved' AND next_status = 'Resolved') OR (status = 'Resolved' AND next_status = 'Unresolved')
ORDER BY caseUri, updated_timestamp;

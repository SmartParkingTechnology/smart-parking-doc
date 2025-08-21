WITH daily_counts AS (
    SELECT 
        
         DATE(caseEvent.timestamp, 'Pacific/Auckland') AS created_date,
        --caseEvent.author.name AS author_name,
        --caseUri,
        COUNT(DISTINCT caseUri) AS casecount,
        COALESCE(
            JSON_VALUE(caseEvent.data, '$.tags.correspondence'),
            JSON_VALUE(caseEvent.data, '$.value') 
        ) AS status,
         ARRAY_AGG(DISTINCT caseEvent.author.name IGNORE NULLS) AS author_names
    FROM `sc-neptune-production.contravention_user_data.user_case_events_scm`
    WHERE DATE(caseEvent.timestamp, 'Pacific/Auckland') 
              BETWEEN PARSE_DATE('%Y%m%d', '20250201')
              AND PARSE_DATE('%Y%m%d', @DS_END_DATE) 
          AND caseEvent.type = 'Tag'
    GROUP BY created_date, status
),

aggregated_counts AS (
    SELECT 
        
       -- caseUri,
        --created_datetime,
        created_date,
        --author_name,
        SUM(CASE WHEN status = 'Unresolved' THEN casecount ELSE 0 END) AS unresolved_cases,
        SUM(CASE WHEN status = 'Resolved' THEN casecount ELSE 0 END) AS resolved_cases
    FROM daily_counts
    GROUP BY  created_date--,author_name
),

cumulative_cases AS (
    SELECT 
        
        --caseUri,
        --created_datetime,
        created_date,
        unresolved_cases,
        resolved_cases,
        SUM(unresolved_cases - resolved_cases) OVER (ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_unresolved
    FROM aggregated_counts
WHERE DATE(created_date) 
              BETWEEN DATE_SUB(PARSE_DATE('%Y%m%d', @DS_START_DATE), INTERVAL 4 DAY)
              AND PARSE_DATE('%Y%m%d', @DS_END_DATE) 
)

--SELECT * FROM cumulative_cases ORDER BY created_date
SELECT 
    cc.*, 
    ARRAY_CONCAT_AGG(dc.author_names) AS author_names
FROM cumulative_cases cc
LEFT JOIN daily_counts dc 
ON cc.created_date = dc.created_date
GROUP BY cc.created_date, cc.unresolved_cases, cc.resolved_cases, cc.cumulative_unresolved
ORDER BY created_date;

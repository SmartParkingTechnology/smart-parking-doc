WITH historical_counts AS (
    SELECT 
        DATE(caseEvent.timestamp, 'Pacific/Auckland') AS created_date,
        COUNT(DISTINCT caseUri) AS casecount,
        COALESCE(
            JSON_VALUE(caseEvent.data, '$.tags.correspondence'),
            JSON_VALUE(caseEvent.data, '$.value') 
        ) AS status
    FROM `sc-neptune-production.contravention_user_data.user_case_events_scm`
    WHERE DATE(caseEvent.timestamp, 'Pacific/Auckland') >= PARSE_DATE('%Y%m%d','20250201') 
          AND caseEvent.type = 'Tag'
    GROUP BY created_date, status
),

historical_aggregated AS (
    SELECT 
        created_date,
        SUM(CASE WHEN status = 'Unresolved' THEN casecount ELSE 0 END) - 
        SUM(CASE WHEN status = 'Resolved' THEN casecount ELSE 0 END) AS cumulative_unresolved_prior
    FROM historical_counts
    GROUP BY created_date
),

historical_cumulative AS (
    -- Compute cumulative sum for historical unresolved cases
    SELECT 
        created_date, 
        SUM(cumulative_unresolved_prior) OVER (ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unresolved
    FROM historical_aggregated
),

last_historical_value AS (
    -- Get the last cumulative unresolved count before the selected range
    SELECT cumulative_unresolved 
    FROM historical_cumulative
    WHERE created_date = (
        SELECT MAX(created_date) 
        FROM historical_cumulative 
        WHERE created_date <= DATE_SUB(PARSE_DATE('%Y%m%d', @DS_START_DATE), interval 1 day) -- Adjusted to capture the last valid date before new records
    )
),

daily_counts AS (
    SELECT 
        DATE(caseEvent.timestamp, 'Pacific/Auckland') AS created_date,
        COUNT(DISTINCT caseUri) AS casecount,
        COALESCE(
            JSON_VALUE(caseEvent.data, '$.tags.correspondence'),
            JSON_VALUE(caseEvent.data, '$.value') 
        ) AS status
    FROM `sc-neptune-production.contravention_user_data.user_case_events_scm`
    WHERE DATE(caseEvent.timestamp, 'Pacific/Auckland') BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE) 
          AND caseEvent.type = 'Tag'
    GROUP BY created_date, status
),

aggregated_counts AS (
    SELECT 
        created_date,
        SUM(CASE WHEN status = 'Unresolved' THEN casecount ELSE 0 END) AS unresolved_cases,
        SUM(CASE WHEN status = 'Resolved' THEN casecount ELSE 0 END) AS resolved_cases
    FROM daily_counts
    GROUP BY created_date
),

cumulative_data AS (
    SELECT 
        created_date,
        unresolved_cases,
        resolved_cases,
        -- Compute cumulative sum properly
        SUM(unresolved_cases - resolved_cases) 
        OVER (ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        + COALESCE((SELECT cumulative_unresolved FROM last_historical_value), 0) 
        AS cumulative_unresolved
    FROM aggregated_counts
)

SELECT * FROM cumulative_data
ORDER BY created_date;

--This CTE is to extract most granual level data with caseUri
WITH daily_counts AS (
SELECT 
        distinct
        caseUri,
        DATETIME(caseEvent.timestamp, 'Pacific/Auckland') AS created_date,
        COUNT(DISTINCT caseUri) AS casecount,
		--To get the Tag and the respective tag Value for each CaseUri
            COALESCE(
            JSON_VALUE(caseEvent.data, '$.tags.correspondence'),
            JSON_VALUE(caseEvent.data, '$.value')
        ) AS status,
    FROM `sc-neptune-production.contravention_user_data.user_case_events_scm`
    WHERE DATE(caseEvent.timestamp, 'Pacific/Auckland') 
          BETWEEN PARSE_DATE('%Y%m%d', '20250804') AND PARSE_DATE('%Y%m%d', '20250804')
          --AND CURRENT_DATE('Pacific/Auckland')  -- or use a dynamic upper bound
          AND caseEvent.type = 'Tag' 
          --AND caseUri IN ('sc://scm/contraventions/parking/c#Parking#0dzkn0#1741121131411','sc://scm/contraventions/parking/c#Parking#07mur6#1741745193925','sc://scm/contraventions/parking/c#Parking#08zdsy#1739742461725','sc://scm/contraventions/parking/c#Parking#16r1pi#1742499647377')
         GROUP BY created_date, status,caseUri,caseEvent.timestamp
    ),
	--This CTE will help to aggregate the data at status level
    final as(
    SELECT 
        created_date,
        caseUri,
        --status,
        SUM(CASE WHEN status = 'Unresolved' THEN casecount ELSE 0 END) AS unresolved_cases,
        SUM(CASE WHEN status = 'Resolved' THEN casecount ELSE 0 END) AS resolved_cases
    FROM daily_counts
    GROUP BY created_date,caseUri
    )


    SELECT 
        created_date,
        caseUri,
        unresolved_cases,
        resolved_cases,
        SUM(unresolved_cases - resolved_cases) OVER (
            ORDER BY created_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_unresolved
    FROM final

    
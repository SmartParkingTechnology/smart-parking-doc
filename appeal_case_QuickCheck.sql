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
        AND caseUri LIKE '%1747605961586%'
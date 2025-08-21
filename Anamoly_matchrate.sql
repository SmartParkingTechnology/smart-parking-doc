WITH processMatch AS (
  SELECT
    org,
    region,
    site,
    DATE(startProcessingTime, 'Pacific/Auckland') AS processingDate,
    SAFE_DIVIDE(SUM(accepted), SUM(total)) AS processMatchRate,
    SUM(total) AS footFall   
    
  FROM `sc-neptune-production.smartcloud.process_metrics`
  WHERE DATE(startProcessingTime, 'Pacific/Auckland') BETWEEN PARSE_DATE('%Y%m%d', '20250101') AND date_sub(PARSE_DATE('%Y%m%d', '20250101'),interval 7 day) and org = 'scm' 
  GROUP BY processingDate, org, region, site
),
windowedData AS (
  SELECT
    *,
    ARRAY_AGG(IFNULL(processMatchRate,0)) OVER (PARTITION BY site ORDER BY processingDate ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS prevRates
  FROM processMatch
)
SELECT
  *,
  PERCENTILE_CONT(processMatchRate, 0.05) OVER (PARTITION BY site ) AS lowerBand,
  PERCENTILE_CONT(processMatchRate, 0.95) OVER (PARTITION BY site ) AS upperBand
FROM windowedData
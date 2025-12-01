-- Step 1: Top 10 Master Brokers by CY DPW (same as before)
DROP TABLE IF EXISTS #TopBrokers;

SELECT TOP 50 BrokerMasterName
INTO #TopBrokers
FROM (
    SELECT 
        b.BrokerMasterName,
        SUM(j.GrossWrittenPremiumRptAmt) AS CY_DPW
    FROM DWH.DIM.DimBroker_SAYA b
    INNER JOIN DWH.dbo.SG_Metric1 j ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2024
      --AND j.CalendarMonth NOT IN ('November')
      --AND b.BrokerMasterName NOT IN ('Inactive','UNKNOWN')
    GROUP BY b.BrokerMasterName
) k
ORDER BY CY_DPW DESC;

-- Step 2: Aggregate NB Metrics for Top 10 Brokers
WITH NB AS (
    SELECT
        SUM(NBGWPRptAmt) AS NB_GWP_Amount,
        SUM(NBWrittenPolicyCnt) AS NB_WrittenPolicyCount
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2024
      --AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (
          SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA 
          WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers)
      )
)
SELECT MetricName, MetricValue
FROM (
    SELECT 
        'Top Brokers NB GWP Amount' AS MetricName, CAST(NB_GWP_Amount AS DECIMAL(18,2)) AS MetricValue
    FROM NB
    UNION ALL
    SELECT 
        'Top Brokers NB Written Policy Count', CAST(NB_WrittenPolicyCount AS DECIMAL(18,2))
    FROM NB
    UNION ALL
    SELECT 
        'Top Brokers New Business Average GWP', 
        CASE WHEN NB_WrittenPolicyCount = 0 THEN NULL ELSE CAST(NB_GWP_Amount * 1.0 / NB_WrittenPolicyCount AS DECIMAL(18,2)) END
    FROM NB
) t;

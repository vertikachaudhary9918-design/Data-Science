-- Step 1: Top 10 Master Brokers by CY DPW
DROP TABLE IF EXISTS #TopBrokers;

SELECT TOP 50 BrokerMasterName
INTO #TopBrokers
FROM (
    SELECT 
        b.BrokerMasterName,
        SUM(j.GrossWrittenPremiumRptAmt) AS CY_DPW
    FROM DWH.DIM.DimBroker_SAYA b
    INNER JOIN DWH.dbo.SG_Metric1 j ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2025
      AND j.CalendarMonth NOT IN ('November')
      --AND b.BrokerMasterName NOT IN ('Inactive','UNKNOWN')
    GROUP BY b.BrokerMasterName
) k
ORDER BY CY_DPW DESC;

-- Step 2: Renewal Metrics for Top 10 Brokers
WITH RNMetrics AS (
    SELECT
        SUM(RenewalGWPRptAmt) AS RN_GWP,
        SUM([RN Written Policy Count]) AS RN_WrittenPolicyCount
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2025
      AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (
          SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA 
          WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers)
      )
)
SELECT MetricName, MetricValue
FROM (
    SELECT 'Top Brokers RN GWP Amount' AS MetricName, CAST(RN_GWP AS DECIMAL(18,2)) AS MetricValue FROM RNMetrics
    UNION ALL
    SELECT 'Top Brokers RN Written Policy Count', CAST(RN_WrittenPolicyCount AS DECIMAL(18,2)) FROM RNMetrics
    UNION ALL
    SELECT 'Top Brokers Avg RN GWP', CAST(CASE WHEN RN_WrittenPolicyCount = 0 THEN NULL ELSE (RN_GWP * 1.0 / RN_WrittenPolicyCount) END AS DECIMAL(18,2)) FROM RNMetrics
) FinalMetrics;

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
    WHERE j.CalendarYear = 2024
      --AND j.CalendarMonth NOT IN ('November')
      --AND b.BrokerMasterName NOT IN ('Inactive','UNKNOWN')
    GROUP BY b.BrokerMasterName
) k
ORDER BY CY_DPW DESC;

-- Step 2: Aggregate Metrics for Top 10 Brokers
WITH BrokerMetrics AS (
    SELECT
        SUM(GrossWrittenPremiumRptAmt) AS DPW,
        SUM([Written Policy Excluding MTC]) AS WrittenPolicyCount,
        SUM(BaseCommissionRptAmt) AS BaseCommission
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2024
      --AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (
          SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA 
          WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers)
      )
),
RetentionMetrics AS (
    SELECT
        SUM(RenewalPremiumAchievedRptAmt) AS RenewalAchievedValue,
        SUM(RenewalPremiumOfferedRptAmt) AS RenewalOfferedValue,
        SUM(RenewalAchievedCnt) AS RenewalAchievedCnt,
        SUM(RenewalOfferedCnt) AS RenewalOfferedCnt
    FROM DWH.dbo.SG_UW_Metric1
    WHERE CalendarYear = 2024-- and CalendarMonth not in ('October')
      AND BrokerSk IN (
          SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA 
          WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers)
      )
)
SELECT MetricName, MetricValue
FROM (
    SELECT 
        'Top Brokers DPW' AS MetricName, CAST(b.DPW AS DECIMAL(18,2)) AS MetricValue
    FROM BrokerMetrics b
    UNION ALL
    SELECT 
        'Top Broker Written Policy Count', CAST(b.WrittenPolicyCount AS DECIMAL(18,2))
    FROM BrokerMetrics b
    UNION ALL
    SELECT 
        'Top Broker Average GWP', CAST(CASE WHEN b.WrittenPolicyCount = 0 THEN NULL ELSE (b.DPW * 1.0 / b.WrittenPolicyCount) END AS DECIMAL(18,2))
    FROM BrokerMetrics b
    UNION ALL
    SELECT 
        'Top Broker Retention % (Premium)', CAST(CASE WHEN r.RenewalOfferedValue = 0 THEN NULL ELSE (r.RenewalAchievedValue * 1.0 / r.RenewalOfferedValue) END AS DECIMAL(18,4))
    FROM RetentionMetrics r
    UNION ALL
    SELECT 
        'Top Broker Retention % (Policy)', CAST(CASE WHEN r.RenewalOfferedCnt = 0 THEN NULL ELSE (r.RenewalAchievedCnt * 1.0 / r.RenewalOfferedCnt) END AS DECIMAL(18,4))
    FROM RetentionMetrics r
    UNION ALL
    SELECT 
        'Top Broker Base Commission', CAST(b.BaseCommission AS DECIMAL(18,2))
    FROM BrokerMetrics b
) FinalMetrics;
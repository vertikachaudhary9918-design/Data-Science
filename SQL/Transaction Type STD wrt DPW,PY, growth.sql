-- Step 1: Get Top 10 Master Brokers by CY DPW
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

-- Step 2: CY and PY Aggregation by Transaction Type for Top 10 Brokers
WITH CY AS (
    SELECT
        'NB' AS TransactionType,
        SUM(NBWrittenPolicyCnt) AS WrittenPolicyCount,
        SUM(NBGWPRptAmt) AS DPW
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2024
      --AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))

    UNION ALL
    SELECT
        'RNL',
        SUM([RN Written Policy Count]),
        SUM(RenewalGWPRptAmt)
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2024
      --AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))

    UNION ALL
    SELECT
        'MTA',
        NULL,
        SUM(NBMidTermAdjRptAmt) + SUM([Renewal MidTermAdjRptAmt])
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2024
     -- AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))

    UNION ALL
    SELECT
        'MTC',
        SUM(NBMidTermCancCnt) + SUM(RNMidTermCancCnt),
        SUM(RenewalMidTermCancelRptAmt) + SUM(NBMidTermCancRptAmt)
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2024
      --AND CalendarMonth NOT IN ('November')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))
),
PY AS (
    SELECT
        'NB' AS TransactionType,
        SUM(NBWrittenPolicyCnt) AS WrittenPolicyCount,
        SUM(NBGWPRptAmt) AS DPW
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2023
      --AND CalendarMonth NOT IN ('November','December')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))

    UNION ALL
    SELECT
        'RNL',
        SUM([RN Written Policy Count]),
        SUM(RenewalGWPRptAmt)
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2023
      --AND CalendarMonth NOT IN ('November','December')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))

    UNION ALL
    SELECT
        'MTA',
        NULL,
        SUM(NBMidTermAdjRptAmt) + SUM([Renewal MidTermAdjRptAmt])
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2023
      --AND CalendarMonth NOT IN ('November','December')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))

    UNION ALL
    SELECT
        'MTC',
         SUM(NBMidTermCancCnt) + SUM(RNMidTermCancCnt),
        SUM(RenewalMidTermCancelRptAmt) + SUM(NBMidTermCancRptAmt)
    FROM DWH.dbo.SG_Metric1
    WHERE CalendarYear = 2023
      --AND CalendarMonth NOT IN ('November','December')
      AND BrokerSk IN (SELECT BrokerKey FROM DWH.DIM.DimBroker_SAYA WHERE BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers))
)

-- Final Output
SELECT
    CY.TransactionType,
    CY.WrittenPolicyCount AS CY_WrittenPolicyCount,
    PY.WrittenPolicyCount AS PY_WrittenPolicyCount,
    CASE WHEN PY.WrittenPolicyCount = 0 THEN NULL
         ELSE ((CY.WrittenPolicyCount - PY.WrittenPolicyCount) * 100.0 /ABS( PY.WrittenPolicyCount))
    END AS PolicyCountGrowth,
    CY.DPW AS CY_DPW,
    PY.DPW AS PY_DPW,
    CASE WHEN PY.DPW = 0 THEN NULL
         ELSE ((CY.DPW - PY.DPW) * 100.0 / ABS(PY.DPW))
    END AS DPW_Growth
FROM CY
LEFT JOIN PY ON CY.TransactionType = PY.TransactionType
ORDER BY CY.TransactionType;
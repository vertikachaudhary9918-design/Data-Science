-- Step 1: Get Top 10 Brokers by CY DPW
DROP TABLE IF EXISTS #TopBrokers;
SELECT TOP 50 BrokerMasterName 
INTO #TopBrokers
FROM (
    SELECT 
        b.BrokerMasterName,
        SUM(j.GrossWrittenPremiumRptAmt) AS DPW
    FROM DWH.DIM.DimBroker_SAYA b
    LEFT JOIN DWH.dbo.SG_Metric1 j ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2024 
      --AND j.CalendarMonth NOT IN ( 'November')
      --AND b.BrokerMasterName NOT IN ('Inactive', 'UNKNOWN')
    GROUP BY b.BrokerMasterName
) k
ORDER BY DPW DESC;
-- Step 2: CY DPW by Broker Segment
WITH CY AS (
    SELECT 
        b.brokersegment,
        SUM(j.GrossWrittenPremiumRptAmt) AS CY_DPW
    FROM DWH.DIM.DimBroker_SAYA b
    LEFT JOIN DWH.dbo.SG_Metric1 j ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2024 
      --AND j.CalendarMonth NOT IN ( 'November')
      AND b.BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers)
    GROUP BY b.brokersegment
),
-- Step 3: PY DPW by Broker Segment
PY AS (
    SELECT 
        b.brokersegment,
        SUM(j.GrossWrittenPremiumRptAmt) AS PY_DPW
    FROM DWH.DIM.DimBroker_SAYA b
    LEFT JOIN DWH.dbo.SG_Metric1 j ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2023 
      --AND j.CalendarMonth NOT IN ( 'November', 'December')
      AND b.BrokerMasterName IN (SELECT BrokerMasterName FROM #TopBrokers)
    GROUP BY b.brokersegment
)
-- Step 4: Final Output with Growth %
SELECT 
    CY.brokersegment,
    CY.CY_DPW,
    PY.PY_DPW,
    CASE 
        WHEN PY.PY_DPW = 0 THEN NULL
        ELSE ((CY.CY_DPW - PY.PY_DPW) * 100.0 / ABS(PY.PY_DPW))
    END AS DPW_GrowthPct
FROM CY
LEFT JOIN PY ON CY.brokersegment = PY.brokersegment
ORDER BY CY.CY_DPW DESC;
 

-- Step 1: Get Top 50 Brokers by DPW (2025)
DROP TABLE IF EXISTS #Top50Brokers;

SELECT TOP 50 BrokerMasterName 
INTO #Top50Brokers
FROM (
    SELECT 
        b.BrokerMasterName,
        SUM(j.WPC) AS DPW
    FROM DWH.DIM.DimBroker_SAYA b
    LEFT JOIN (
        SELECT 
            BrokerSk,
            SUM(GrossWrittenPremiumRptAmt) AS WPC
        FROM DWH.dbo.SG_Metric1
        WHERE CalendarYear = 2024 
          --AND CalendarMonth NOT IN ( 'November')
        GROUP BY BrokerSk
    ) j ON j.BrokerSk = b.BrokerKey
    --WHERE b.BrokerMasterName NOT IN ('Inactive', 'UNKNOWN')
    GROUP BY b.BrokerMasterName
) k
ORDER BY DPW DESC;

-- Step 2: CY DPW by Line of Business for Top 50 Brokers
WITH CY_LOB AS (
    SELECT 
        j.LineOfBusinessName,
        SUM(j.GrossWrittenPremiumRptAmt) AS CY_DPW
    FROM DWH.dbo.SG_Metric1 j
    INNER JOIN DWH.DIM.DimBroker_SAYA b ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2024 
      --AND j.CalendarMonth NOT IN ( 'November')
      AND b.BrokerMasterName IN (SELECT BrokerMasterName FROM #Top50Brokers)
    GROUP BY j.LineOfBusinessName
),

-- Step 3: PY DPW by Line of Business for Top 50 Brokers
PY_LOB AS (
    SELECT 
        j.LineOfBusinessName,
        SUM(j.GrossWrittenPremiumRptAmt) AS PY_DPW
    FROM DWH.dbo.SG_Metric1 j
    INNER JOIN DWH.DIM.DimBroker_SAYA b ON j.BrokerSk = b.BrokerKey
    WHERE j.CalendarYear = 2023 
      --AND j.CalendarMonth NOT IN ( 'November','December')
      AND b.BrokerMasterName IN (SELECT BrokerMasterName FROM #Top50Brokers)
    GROUP BY j.LineOfBusinessName
)

-- Step 4: Combine CY, PY and Calculate Growth
SELECT 
    CY_LOB.LineOfBusinessName,
    CY_LOB.CY_DPW,
    PY_LOB.PY_DPW,
    CASE 
        WHEN PY_LOB.PY_DPW = 0 THEN NULL
        ELSE ROUND((CY_LOB.CY_DPW - PY_LOB.PY_DPW) * 100.0 / PY_LOB.PY_DPW, 2)
    END AS Growth_Percent
FROM CY_LOB
LEFT JOIN PY_LOB ON CY_LOB.LineOfBusinessName = PY_LOB.LineOfBusinessName
ORDER BY CY_LOB.CY_DPW DESC;
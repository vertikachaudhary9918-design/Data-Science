-- Step 1: Get Top 50 Brokers by CY DPW (2025)
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
    GROUP BY b.BrokerMasterName
) k
ORDER BY DPW DESC;

-- Step 2: CY DPW, PY DPW, DPW Growth, and Written Policy Count excluding MTC
SELECT 
    b.BrokerMasterName,

    -- CY DPW
    SUM(CASE 
        WHEN j.CalendarYear = 2024-- AND j.CalendarMonth NOT IN ( 'November') 
        THEN j.GrossWrittenPremiumRptAmt 
        ELSE 0 
    END) AS CY_DPW,

    -- PY DPW
    SUM(CASE 
        WHEN j.CalendarYear = 2023-- AND j.CalendarMonth NOT IN ('December', 'November') 
        THEN j.GrossWrittenPremiumRptAmt 
        ELSE 0 
    END) AS PY_DPW,

    -- DPW Growth
    CASE 
        WHEN SUM(CASE WHEN j.CalendarYear = 2023-- AND j.CalendarMonth NOT IN ('December', 'November') 
                      THEN j.GrossWrittenPremiumRptAmt ELSE 0 END) = 0 
        THEN NULL
        ELSE 
            (SUM(CASE WHEN j.CalendarYear = 2024-- AND j.CalendarMonth NOT IN ( 'November') 
                      THEN j.GrossWrittenPremiumRptAmt ELSE 0 END) - 
             SUM(CASE WHEN j.CalendarYear = 2023-- AND j.CalendarMonth NOT IN ('December', 'November') 
                      THEN j.GrossWrittenPremiumRptAmt ELSE 0 END)) / 
             (SUM(CASE WHEN j.CalendarYear = 2023-- AND j.CalendarMonth NOT IN ('December', 'November') 
                      THEN j.GrossWrittenPremiumRptAmt ELSE 0 END)) *100
    END AS DPW_Growth,

    -- Written Policy Count excluding MTC
SUM(CASE 
        WHEN j.CalendarYear = 2024-- AND j.CalendarMonth NOT IN ( 'November') 
        THEN j.[Written Policy Excluding MTC] 
        ELSE 0 
    END) AS WrittenPolicyCount_Excl_MTC

FROM DWH.dbo.SG_Metric1 j
LEFT JOIN DWH.DIM.DimBroker_SAYA b ON j.BrokerSk = b.BrokerKey
WHERE b.BrokerMasterName IN (SELECT BrokerMasterName FROM #Top50Brokers)
GROUP BY b.BrokerMasterName
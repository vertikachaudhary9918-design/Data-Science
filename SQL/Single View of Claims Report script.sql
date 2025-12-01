
-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 0 -- Declare and set the start date of the current month
-- --------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE @monthstartdate DATETIME; 
-- SET @monthstartdate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);
SET @monthstartdate = '2025-01-01';

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1 -- Gather Fields From ODS & DWH Database Tables 
-- --------------------------------------------------------------------------------------------------------------------------------------------------
	drop table if exists #ODS_DWH_Data

	Select
		distinct 
		Claim.ClaimKey,
		Claim.ClaimNumber,
		case 
			when Claim.SourceName = 'Genius' and Product.ProductCode like 'NI%' then 'NI Genius'
			when Claim.SourceName = 'Genius' and Product.ProductCode not like 'NI%' then 'Genius'
			when Claim.SourceName = 'Benchmark' then 'Benchmark'
			when Claim.Claimnumber in ('C0xxxx','E1xxxx') then 'Arev'
		end as [System],
		Case 
			when Claim.ClaimStatusCurrent_Genius is NULL then 'Open'
			when Claim.ClaimStatusCurrent_Genius = 'Open' then 'Open' 
			when Claim.ClaimStatusCurrent_Genius = 'Settled' then 'Closed'
			else 'UKN' 
		End as 'Claim Status',
		-- Claim.SourceName,
		Source.SourceName as PolicySourceName,
		Claim.ClaimAccidentDate,
		Claim.ClaimReportDate,
		Claim.ClaimDescription,     
		rank() over (partition by ClaimNumber order by ClaimReportDate desc,Claim.BatchNumber asc) as 'Rank_Claim',
		case 
			when Claim.ClaimStatusSk = '2' then NULL 
			when Claim.ClaimSettlementDt_GeniusSystem is null then ClaimSettlementDate else ClaimSettlementDt_GeniusSystem
		End as 'ClaimsettlementDate',
		Claim.Resql2,
		MAX(CASE WHEN ClaimTransactionType.transactiontypecode IN ('O/S') THEN ClaimTransaction.TransactionDate ELSE 0 END) OVER (PARTITION BY claim.claimnumber) AS 'Date_Reserve_Last_Updated',
		Policy.CustomerPolicyNr,
		Product.ProductCode,
		Broker.brokercode,
		ClaimDetails.Claimantfullname as 'Claimant',
		ClaimHandler.claimhandlercd,
		CASE WHEN Claim.ClaimNumber like '%/%/%' THEN 'Benchmark' 
		ELSE ClaimHandler.ClaimHandlerFirstName + ' ' + ClaimHandler.ClaimHandlerSurname END AS [Claim Handler Name],
		broker.BrokerGroup as [Broker Group],
		ClaimHandlerTeam.ClaimHandlerTeamCd as 'Team',
		ClaimCircumstances.CircumstancesCode as 'Cause of Claim',
		Benchmark.[POLICY HOLDER] as 'Benchmark Insured',

		
		
		
		case 
			when Product.ProductCode like 'NI%' THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecode in ('REC') and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ClaimTransaction.claimtransactionamountlocal,0) ELSE 0 END)  OVER (PARTITION BY Claim.Claimkey)
			else SUM(CASE WHEN ClaimTransactionType.transactiontypecode in ('REC') and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ClaimTransaction.claimtransactionamountreporting,0) ELSE 0 END)  OVER (PARTITION BY Claim.Claimkey)
		end AS 'Gross Recoveries Received',	

		CASE 
			WHEN Product.ProductCode like 'NI%' then 'NI' 
			else 'ROI'
		end as [ROI/NI]

	into #ODS_DWH_Data

	From
	--[DWH_ARCHIVE].Fact.[FactClaim_v45.0_20240802] Claim -- Archived Table From Last Month
	 DWH.Fact.FactClaim Claim

	 LEFT JOIN dwh.Fact.FactClaimTransaction ClaimTransaction 
	--LEFT JOIN [DWH_ARCHIVE].[Fact].[FactClaimTransaction_v45.0_20240802] ClaimTransaction -- Archived Table From Last Month
		ON ClaimTransaction.ClaimSK = claim.ClaimKey

	LEFT JOIN ODS.ODS.claimtransaction ODS_ClaimTransaction
		on ClaimTransaction.ClaimTransactionKey=ODS_ClaimTransaction.ClaimTransactionsk
		and ClaimTransaction.ClaimSK=ODS_ClaimTransaction.ClaimSK
		and ODS_ClaimTransaction.CurrentRow=1

	-- Join with Transaction Type Table to derive the type of each transaction
    LEFT JOIN dwh.dim.DimClaimTransactionType ClaimTransactionType 
		ON ClaimTransaction.TransactionTypeSK =ClaimTransactionType.TransactionTypeKey 

    -- Join with Policy Information
    LEFT JOIN ODS.ODS.Policy Policy 
		ON claim.PolicySK = Policy.PolicySK 
		AND Policy.CurrentRow='1'

	-- Join with Policy Source
    LEFT JOIN ODS.ODS.Source Source 
		ON Source.SourceKey = Policy.SourceSystemSk 
		AND Policy.CurrentRow='1'

	-- Join with Product Info
    LEFT JOIN ODS.ODS.Product Product 
		ON Product.ProductSK = Policy.ProductSK
		and Product.ProductSource=Source.SourceName
		AND Product.CurrentRow='1'

	-- Join with Broker Table
    LEFT JOIN ODS.ODS.Broker Broker 
		ON Broker.CurrentRow = '1' 
		AND Policy.BrokerSK = Broker.BrokerSK 

	-- Join with Claim Details
    LEFT JOIN ODS.ODS.ClaimDetails ClaimDetails 
		ON ClaimDetails.Claimsk = claim.ClaimKey 
		and ClaimDetails.CurrentRow = '1' 
	
	-- Join the Claim Handler
    LEFT JOIN dwh.dim.DimClaimHandler ClaimHandler 
		ON ClaimHandler.ClaimHandlerKey = Claim.HandlerSK 

	-- Join the Claim Handler Team
	LEFT JOIN DWH.DIM.DimClaimHandlerTeam ClaimHandlerTeam 
		on ClaimHandlerTeam.ClaimHandlerTeamKey = claim.HandlerTeamSK

    -- Join with Section Table
	LEFT JOIN dwh.dim.DimClaimCircumstances ClaimCircumstances 
		on claim.CircumstancesSK = ClaimCircumstances.CircumstancesKey

	-- Join table for Benchmark Insured
		left join
		 ODS.Benchmark.SI_ClaimsAllProducts benchmark
		 on benchmark.CLAIMSREFERENCE =Claim.ClaimNumber and Benchmark.CurrentRow ='1'

	
	WHERE
		
		YEAR(claim.ClaimReportDate) >= '2000' 
		AND NOT(claim.SourceName = 'Benchmark' AND claim.ClaimDescription LIKE '%liability%'
		and claim.ClaimStatusCurrent_Genius <> ('Settled')
		)

	Group by 
		Claim.ClaimKey,
		Claim.ClaimNumber,
		Claim.SourceName,
		Claim.ClaimAccidentDate,
		Claim.ClaimReportDate,
		Claim.ClaimDescription,
		Claim.ClaimSettlementDt_GeniusSystem,ClaimSettlementDate,
		Claim.ClaimStatusSk,
		Claim.ClaimStatusCurrent_Genius,
		Claim.BatchNumber,
		Claim.RESQL2,
		Claim.BrokerSk,
		ClaimTransaction.TransactionDate,
		ClaimTransactionType.TransactionTypeCode,
		Policy.CustomerPolicyNr,
		Source.SourceName,
		Product.ProductCode,
		Broker.Brokercode,broker.BrokerGroup,
		ClaimDetails.Claimantfullname,
		ClaimHandler.claimhandlercd,ClaimHandler.ClaimHandlerFirstName,ClaimHandler.ClaimHandlerSurname,
		ClaimHandlerTeam.ClaimHandlerTeamCd,
		ClaimCircumstances.CircumstancesCode,
		Benchmark.[POLICY HOLDER],
		ClaimTransaction.claimtransactionamountlocal,
		ClaimTransaction.claimtransactionamountreporting,
		ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd,
		ClaimTransaction.ClaimTransactionKey
	-- (287679 rows affected) -- 00:24


	create index idx1 on #ODS_DWH_Data(CustomerPolicyNr);
	create index idx2 on #ODS_DWH_Data(ClaimNumber);

	-- select * from #ODS_DWH_Data
	-- select * from [DWH_SANDBOX].dbo.[ROI_OCD_20240731] 
	
-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 2 -- Gather Fields From ODS & DWH Database Tables At Section Level - Claim Peril
-- --------------------------------------------------------------------------------------------------------------------------------------------------	

	drop table if exists #ODS_DWH_Data_SectionLevel_Peril

	select 

		ODS_DWH_Data_SectionLevel_Peril.ClaimKey,
		ODS_DWH_Data_SectionLevel_Peril.ClaimPerilDescription
	
	INTO #ODS_DWH_Data_SectionLevel_Peril

	From 
	
	(
		select 
			distinct 
			ODS_DWH_Data_SectionLevel_Peril_Detail.ClaimKey,
			ODS_DWH_Data_SectionLevel_Peril_Detail.ClaimPerilDescription,
			ODS_DWH_Data_SectionLevel_Peril_Detail.Ranking,
			MIN(ODS_DWH_Data_SectionLevel_Peril_Detail.Ranking) over (partition by ODS_DWH_Data_SectionLevel_Peril_Detail.ClaimKey) as MinRank 
	
		from 
		(


		SELECT 
			Claim.ClaimKey,
			ClaimPeril.ClaimPerilDescription,
			(
				CASE 
					WHEN ClaimPeril.ClaimPerilDescription = 'TPI' THEN 1
					WHEN ClaimPeril.ClaimPerilDescription = 'LIAB' THEN 2
					WHEN ClaimPeril.ClaimPerilDescription = 'Ground up'THEN 3
					WHEN ClaimPeril.ClaimPerilDescription = 'TPD'THEN 4
					WHEN ClaimPeril.ClaimPerilDescription = 'ADFT'THEN 5
					WHEN ClaimPeril.ClaimPerilDescription='Neg Deductible' THEN 6
					WHEN ClaimPeril.ClaimPerilDescription='ROI Thomond'THEN 7
					WHEN ClaimPeril.ClaimPerilDescription='Escape of Water'THEN 8
					WHEN ClaimPeril.ClaimPerilDescription='Other Damage'THEN 9
					WHEN ClaimPeril.ClaimPerilDescription='Other'THEN 10
					WHEN ClaimPeril.ClaimPerilDescription='Subsidence'THEN 11
					WHEN ClaimPeril.ClaimPerilDescription='Storm'THEN 12
					WHEN ClaimPeril.ClaimPerilDescription='Fire'THEN 13
					WHEN ClaimPeril.ClaimPerilDescription='Theft'THEN 14
					WHEN ClaimPeril.ClaimPerilDescription='Accidental Damage'THEN 15
					WHEN ClaimPeril.ClaimPerilDescription='All Causes'THEN 16
					WHEN ClaimPeril.ClaimPerilDescription='Weather'THEN 17
					WHEN ClaimPeril.ClaimPerilDescription='BORD'THEN 18
					WHEN ClaimPeril.ClaimPerilDescription='WS'THEN 19
					WHEN ClaimPeril.ClaimPerilDescription='Flood'THEN 20
					WHEN ClaimPeril.ClaimPerilDescription='EIR'THEN 21
					WHEN ClaimPeril.ClaimPerilDescription='Escape of Oil'THEN 22
					WHEN ClaimPeril.ClaimPerilDescription='Musgraves'THEN 23
					WHEN ClaimPeril.ClaimPerilDescription='NA'THEN 24
					WHEN ClaimPeril.ClaimPerilDescription='NI Thomond'THEN 25
					WHEN ClaimPeril.ClaimPerilDescription='NI Aggregate'THEN 26
				END
			) AS Ranking

			FROM
			DWH.Fact.FactClaim Claim
			join #ODS_DWH_Data ODS_DWH_Data
				on Claim.ClaimKey = ODS_DWH_Data.ClaimKey 
			left join DWH.Fact.FactClaimSection ClaimSection
				on Claim.ClaimKey = ClaimSection.ClaimSK
			left join DWH.DIM.DimClaimPeril ClaimPeril
				on ClaimPeril.ClaimPerilKey = ClaimSection.ClaimPerilSK 
			WHERE 
				YEAR(Claim.ClaimReportDate) >= '2000' 
				AND NOT(Claim.SourceName = 'Benchmark' AND Claim.ClaimDescription LIKE '%liability%'
				and claim.ClaimStatusCurrent_Genius <> ('Settled')
				)
				 
				-- and Claim.ClaimKey=1345327

			) as ODS_DWH_Data_SectionLevel_Peril_Detail

		) as ODS_DWH_Data_SectionLevel_Peril

		where ODS_DWH_Data_SectionLevel_Peril.Ranking=ODS_DWH_Data_SectionLevel_Peril.MinRank

	
	create index idx1 on #ODS_DWH_Data_SectionLevel_Peril(ClaimKey);



	-- (281967 rows affected) -- 00:00



--Gross OS
drop table if exists #ODS_Gross_OS
		Select
			distinct Claim.Claimsk,
			Claim.ClaimNumber,
			Product.ProductCode ,
			case 
			 
		when AGA137 like 'N%' THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimSourceTransactionID <> '0'  and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_ClaimTransaction.ClaimTransactionAmtLcl,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
		When AGA137 not like 'N%' or AGA137 is NULL THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_ClaimTransaction.ClaimTransactionAmtRpt,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
		when  claimnumber like ('%/%/%') then SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 then isnull(ODS_ClaimTransaction.ClaimTransactionAmtRpt,0) ELSE 0 END) OVER (PArtITION BY Claim.Claimnumber) ELSE 0 
	
		--	when Product.ProductCode like 'NI%' THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimSourceTransactionID <> '0'  and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_ClaimTransaction.ClaimTransactionAmtLcl,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
		--When Product.ProductCode not like 'NI%' THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_ClaimTransaction.ClaimTransactionAmtRpt,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
		--when  claimnumber like ('%/%/%') and ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 then isnull(ODS_ClaimTransaction.ClaimTransactionAmtRpt,0) ELSE 0 

				--when Product.ProductCode like 'NI%' THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimSourceTransactionID <> '0'  and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_ClaimTransaction.ClaimTransactionAmtLcl,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
				--else SUM(CASE WHEN ClaimTransactionType.transactiontypecd = 'O/S' and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_ClaimTransaction.ClaimTransactionAmtRpt,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
			end as 'Gross O/S'
 
		 into #ODS_Gross_OS
	
			From

			 ODS.ODS.Claim Claim 
			LEFT JOIN DWH.Fact.FactClaimTransaction  ClaimTransaction
					ON ClaimTransaction.ClaimSK = claim.Claimsk and claim.CurrentRow = 1
 
				LEFT JOIN ODS.ODS.claimtransaction ODS_ClaimTransaction
					on ClaimTransaction.ClaimTransactionKey=ODS_ClaimTransaction.ClaimTransactionsk
					and ClaimTransaction.ClaimSK=ODS_ClaimTransaction.ClaimSK
					and ODS_ClaimTransaction.CurrentRow=1
					Left join ODS.GEnius.SI_ZKFA ZKFA
		on ZKFA.FAFAMR = Claim.ClaimNumber and ZKFA.CurrentRow = 1
		left join ODS.genius.SI_ZNAG ZNAG
		on ZKFA.FAFALB = ZNAG.AGNACD  and ZNAG.CurrentRow = 1

 
				-- Join with Transaction Type Table to derive the type of each transaction
				LEFT JOIN ODS.ODS.ClaimTransactionType ClaimTransactionType 
					ON ClaimTransaction.TransactionTypeSK =ClaimTransactionType.TransactionTypeSK 
					-- Join with Policy Information
				LEFT JOIN ODS.ODS.Policy Policy 
					ON claim.PolicySK = Policy.PolicySK 
					AND Policy.CurrentRow='1'
 
				-- Join with Policy Source
				LEFT JOIN ODS.ODS.Source Source 
					ON Source.SourceKey = Policy.SourceSystemSk 
					AND Policy.CurrentRow='1'
					-- Join with Product Info
				LEFT JOIN ODS.ODS.Product Product 
					ON Product.ProductSK = Policy.ProductSK
					and Product.ProductSource=Source.SourceName
					AND Product.CurrentRow='1'
				WHERE
   
				ClaimTransactionTypSK  = 2 
				--and ODS_ClaimTransaction.ClaimSourceTransactionID <> 0
				and ClaimTransactionEstimateRecoveryInd=0 
				and ClaimTransactionReInsuranceInd=0 
				and ODS_Claimtransaction.CurrentRow = 1
				and ClaimTransactionDt < @monthstartdate 
				and
				YEAR(claim.ClaimReportDt) >= '2000'  and claim.BatchNumber <> '22222222' and
				claim.BatchNumber <> '33333333'
				--and ODS_ClaimTransaction.BatchNumber <> '22222222' 
				--and ODS_ClaimTransaction.BatchNumber <> '33333333'
		 		and ClaimTransaction.TransactionDate<=Eomonth(DATEADD(month,-1,GETDATE()))
	-- group by Claim.claimsk,ClaimNumber,TransactionTypeCd,ClaimTransactionEstimateRecoveryInd,ProductCode,ClaimNumber,claimtransactionAmtrpt,ClaimTransactionAmtLcl
	--,ODS_claimtransaction.ClaimSourceTransactionID

	create index idx1 on #ODS_Gross_OS(Claimsk);

--Total Net Ex Rex
Drop table if exists #total_Nett_exrex
			--Total Net O/S Ex Rex
			SELECT 
			c.ClaimNumber as 'ClaimNumber',
			sum(isnull(t.G3F8MB,0))*-1 as	Total_Net_OS_EX_REX
			into #total_Nett_exrex
			FROM
			(select distinct c.ClaimNumber from DWH.Fact.FactClaim c)as c


			  LEFT JOIN

			  (
			  select a.FAFAMR,
			  zkg.G3G3PO,
			  zkg.G3G3M2,
			  zkg.G3OLDT,
			  zkg.G3F8MB
			  from
			ODS.Genius.SI_ZKFA a
			  --ON c.ClaimNumber=a.FAFAMR 
			 -- AND a.CurrentRow='1'--joining on claim master ref

			  LEFT JOIN

			ODS.Genius.SI_ZKG0 zk
			  ON a.FAFANO=zk.G0FANO 
			  AND zk.CurrentRow='1'--claim master code

			  LEFT JOIN 

			ODS.Genius.SI_ZKG3 zkg
			  ON zk.G0FANO=zkg.G3FANO 
			  AND zk.G0FBCD=zkg.G3FBCD 
			  AND zk.G0F3CD=zkg.G3F3CD 
			  AND zkg.CurrentRow='1' 
  
			  where a.CurrentRow='1' and 
			   zkg.G3G3PO='0'                         --CltrAmt Payment or O/S?
					  --AND  zkg.G3G3M2!='REX'                      --CltrAmt Mvmt type 2 code
					  AND  zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))
			) as t
			on c.ClaimNumber=t.FAFAMR

			--where c.ClaimNumber=''
			group by c.ClaimNumber

		  

create index idx1 on #total_Nett_exrex(ClaimNumber);


	--Date loss adjuster appointed
	Drop table if exists #DWH_DateLoss_Adjuster

					select 
					c.ClaimNumber as 'ClaimNumber',
					max(Case when zuni.NINUCD='LAD' then (zuni.NINISD) else '' end)
					as 'Date Loss Adjuster appointed'
					into #DWH_DateLoss_Adjuster
					from
					DWH.fact.FactClaim c
					left join
					ODS.Genius.SI_ZKFA a --claim level table in genius
					on c.ClaimNumber=a.FAFAMR and a.CurrentRow='1'
					left join
					ODS.Genius.SI_ZUNI zuni
					on a.FAFANO=zuni.NIFANO and zuni.CurrentRow='1' 
					--and zuni.NINUCD='LAD'
					where zuni.NINISD<=Eomonth(DATEADD(month,-1,GETDATE())) 
					--and c.ClaimNumber=''
					group by c.ClaimNumber
					order by c.ClaimNumber

create index idx1 on #DWH_DateLoss_Adjuster(ClaimNumber);



-- LOI FEE
 Drop table if exists #ODS_LOI_FEE

		Select ClaimNumber,G3F8Tb,G3F8To,AGA137,G3fbcd,g3f3cd,[Latest Trs date],case 
			 
				when AGA137 like 'N%' THEN G3F8TO *-1
				When AGA137 not like 'N%' or AGA137 is NULL THEN G3F8TB*-1
  End as 'LOI FEE'
		,G3G3M1,G3G3M2,G3AMCD,G3G3PO--,g3fbcd,g3f3cd
		,g3oldt--,[Lowest Trs Code]
		into #ODS_LOI_FEE
	
		from DWH.Fact.FactClaim c
		left join(
		Select G0G0OR, G3F8TB,G3F8To,G3F8MO,Rank() over (partition by g0g0or order by g3fbcd,G3Oldt desc,g3f3cd desc) as 'Latest Trs date',g3fbcd,g3f3cd,
		AGA137,G3OLDT,G3G3M1,G3G3M2,G3AMCD,G3G3PO 
		from ODS.GEnius.SI_ZKG0 o
		join ODS.GENIUS.SI_ZKG3 g

		on o.G0FANO = g.G3FANO and o.G0FBCD = g.G3FBCD and o.G0F3CD = g.G3F3CD 
		left join ODS.GEnius.SI_ZKFA ZKFA
				on ZKFA.FAFAMR = o.G0G0OR and ZKFA.CurrentRow = 1
				left join ODS.genius.SI_ZNAG ZNAG
				on ZKFA.FAFALB = ZNAG.AGNACD  and ZNAG.CurrentRow = 1 

		--where G3FANO = '1400071'

		where G3G3M1 = 'FEE' --Checking if ClTrAmt Mvmt Type 1 code = REC 
		and G3G3M2 = 'LOI'
		and G3AMCD = 'FEE'
		and G3G3PO = 1	   --and CltrAmt Payment or O/S = 0
		and (G0F3MN = ' ' or  G0F3MN is null )
		and G3OLDT < '2025-01-01'--DATEADD(DAY,-DAY(GetDate()), GETDATE())
		--CONVERT(date,stuff(STUFF(g.JODATE,5,0,'-'),3,0,'-'),3)
		and g.currentRow = '1' and o.currentrow ='1'
		--where G3FANO in ('') 
		--where G0G0Or in ('')
		group by G0G0OR,G3F8TB,G3F8To,G3F8MO,G3F3CD,AGA137,G3OLDT,G3G3M1,G3G3M2,G3AMCD,G3G3PO,g3fbcd,g3f3cd) as kl on kl.G0G0OR = c.ClaimNumber
			group by ClaimNumber,[Latest Trs date],G3F8TB,G3F8To,AGA137,G3G3M1,G3G3M2,G3AMCD,G3G3PO,g3fbcd,g3f3cd,g3oldt--,[Lowest Trs Code]
		--having --[Latest Trs date] = max(g3oldt) and
		--G3FBCD = min(g3fbcd)
 having [Latest Trs date] = 1
		order by ClaimNumber, G3FBCD asc, G3OLDT desc

create index idx1 on #ODS_LOI_FEE(Claimnumber);


--Recovery Reserve
Drop table if exists #ODS_Recovery_Reserve
					SELECT 
					c.ClaimNumber,
					(SUM((CASE 
								WHEN zkg.G3AMCD = 'O/S' 
								AND zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))
								AND zkg.G3G3M1 = 'REC' 
								AND zkg.G3G3PO = '0'   
								AND CONCAT(G0CTHA,G0G0P2) NOT IN (32)
								AND CONCAT(G0CTHA,G0G0P2)NOT IN (33) 
								AND G0CTHA NOT IN (2)
								AND G0CTHA NOT IN (0)

								--THEN cast(isnull(zkg.G3F8MB,0) as int) 
								THEN (isnull(zkg.G3F8MB,0)) 
								ELSE 0 
								END)))*(-1) AS rec
					into #ODS_Recovery_Reserve
					FROM
					DWH.Fact.FactClaim c

					LEFT JOIN

					ODS.Genius.SI_ZKFA a
					on c.ClaimNumber=a.FAFAMR 
					  AND a.CurrentRow='1'--joining on claim master ref

					LEFT JOIN

					ODS.Genius.SI_ZKG0 zk
					on a.FAFANO=zk.G0FANO 
					  AND zk.CurrentRow='1'--claim master code

					LEFT JOIN 

					ODS.Genius.SI_ZKG3 zkg
					on zk.G0FANO=zkg.G3FANO 
					  AND zk.G0FBCD=zkg.G3FBCD 
					  AND zk.G0F3CD=zkg.G3F3CD 
					  AND zkg.CurrentRow='1' --joining on Claim master code,Claim section code and Claim Trans hdr code
					--where c.ClaimNumber in ('')

					group by c.ClaimNumber

create index idx1 on #ODS_Recovery_Reserve(ClaimNumber);

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 3 -- Gather Fields From ODS claimostransbreakup Table -- This contains records that aren't in the DWH Tables
-- --------------------------------------------------------------------------------------------------------------------------------------------------
	-- DECLARE @monthstartdate DATETIME; 
	-- SET @monthstartdate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);
	-- SET @monthstartdate = '2024-08-01';

	drop table if exists #ODS_Claimostransbreakup
	
	SELECT 
		claimostransbreakup.claimsk,

		case 
			when Product.ProductCode like 'NI%' THEN SUM(isnull(claimostransbreakup.Lossreserveamtlcl*-1,0))
			else SUM(isnull(claimostransbreakup.Lossreserveamtrpt*-1,0)) 
		end AS 'Lossreserveamt',		

		case 
			when Product.ProductCode like 'NI%' THEN SUM(isnull(claimostransbreakup.Recoveryreserveamtlcl*-1,0))
			else SUM(isnull(claimostransbreakup.Recoveryreserveamtrpt*-1,0)) 
		end AS 'Recoveryreserveamt',		


		case 
			when Product.ProductCode like 'NI%' THEN SUM(isnull(claimostransbreakup.ClaimTransactionAmtLcl*-1,0))
			else SUM(isnull(claimostransbreakup.ClaimTransactionAmtrpt*-1,0)) 
		end AS 'ClaimTransactionAmt'
		
	into #ODS_Claimostransbreakup
    
	FROM 
	
	ODS.dbo.claimostransbreakup claimostransbreakup 

    LEFT JOIN ODS.ODS.Claim Claim
		on Claim.claimsk=claimostransbreakup.claimsk
		and Claim.currentrow=1

	    -- Join with Policy Information
    LEFT JOIN ODS.ODS.Policy Policy 
		ON claim.PolicySK = Policy.PolicySK 
		AND Policy.CurrentRow='1'

	-- Join with Policy Source
    LEFT JOIN ODS.ODS.Source Source 
		ON Source.SourceKey = Policy.SourceSystemSk 
		AND Policy.CurrentRow='1'

	-- Join with Product Info
    LEFT JOIN ODS.ODS.Product Product 
		ON Product.ProductSK = Policy.ProductSK
		and Product.ProductSource=Source.SourceName
		AND Product.CurrentRow='1'
    
	WHERE 
		claimostransbreakup.claimtransactiondt < @monthstartdate 
		AND ClaimTransactionEstimateRecoveryInd = '0' 
		-- AND claimtransactionreinsuranceind = '0' 

	GROUP BY 
		claimostransbreakup.claimsk,Product.ProductCode

	create index idx1 on #ODS_Claimostransbreakup(claimsk);

	-- (1324461 rows affected) -- 00:05
	-- (1324461 rows affected) -- 01:09

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 4 -- Gather Recovery Reserve From ODS ClaimTransaction Table
-- --------------------------------------------------------------------------------------------------------------------------------------------------
	drop table if exists #ODS_ClaimTransaction_RecoveryReserve

	select 

		--ODS_ClaimTransaction_RecoveryReserve.claimsk,
		ODS_ClaimTransaction_RecoveryReserve.ClaimNumber,
		case 
			when ODS_ClaimTransaction_RecoveryReserve.ProductCode like 'NI%' THEN SUM(isnull(ODS_ClaimTransaction_RecoveryReserve.ClaimTransactionAmtLcl*-1,0))
			else SUM(isnull(ODS_ClaimTransaction_RecoveryReserve.ClaimTransactionAmtrpt*-1,0)) 
		end AS 'ClaimTransactionAmt'

	into #ODS_ClaimTransaction_RecoveryReserve

	from

	(
		select 
			ClaimTransaction.ClaimTransactionSK,
			ClaimTransaction.claimsk,
			Claim.ClaimNumber,
			ClaimTransaction.ClaimSectionSk,
			Product.ProductCode,
			ClaimTransaction.ClaimTransactionTypSK,
			ClaimTransactionType.TransactionTypeCd,
			ClaimTransactionType.TransactionTypeName,
			ClaimTransactionType.TransactionTypeSourceValue,
			ClaimTransaction.ClaimPaymentTypSK,
			ClaimPaymentType.PaymentTypeName,
			ClaimTransaction.ClaimSourceTransactionID,
			ClaimTransaction.ClaimTransactionDt,
			ClaimTransaction.ClaimTransactionAmtLcl,
			ClaimTransaction.ClaimTransactionAmtRpt,
			ClaimTransaction.ClaimTransactionEstimateRecoveryInd,
			ClaimTransaction.ClaimTransactionReInsuranceInd

		from 
			ods.ClaimTransaction ClaimTransaction

			left join ods.ClaimTransactionType ClaimTransactionType
				on ClaimTransaction.ClaimTransactionTypSK=ClaimTransactionType.TransactionTypeSK
				and ClaimTransaction.ClaimTransactionDt<@monthstartdate

			left join ods.ClaimPaymentType ClaimPaymentType
				on ClaimTransaction.ClaimPaymentTypSK=ClaimPaymentType.PaymentTypeSK

			LEFT JOIN ODS.ODS.Claim Claim
				on Claim.claimsk=ClaimTransaction.claimsk
				and Claim.currentrow=1

			-- Join with Policy Information
			LEFT JOIN ODS.ODS.Policy Policy 
				ON claim.PolicySK = Policy.PolicySK 
				AND Policy.CurrentRow='1'

			-- Join with Policy Source
			LEFT JOIN ODS.ODS.Source Source 
				ON Source.SourceKey = Policy.SourceSystemSk 
				AND Policy.CurrentRow='1'

			-- Join with Product Info
			LEFT JOIN ODS.ODS.Product Product 
				ON Product.ProductSK = Policy.ProductSK
				and Product.ProductSource=Source.SourceName
				AND Product.CurrentRow='1'

			join #ODS_DWH_Data ODS_DWH_Data
			 	on ClaimTransaction.claimsk = ODS_DWH_Data.ClaimKey 

			where 
				ClaimTransaction.CurrentRow=1
				and ClaimTransaction.ClaimTransactionEstimateRecoveryInd=1 -- REX
				-- and ClaimTransactionType.TransactionTypeCd in ('REC')
				and ClaimTransactionType.TransactionTypeName='Reserve'
				-- and Claim.ClaimNumber='202310001975'
				
		) as ODS_ClaimTransaction_RecoveryReserve

		group by 
			--ODS_ClaimTransaction_RecoveryReserve.claimsk,
			ODS_ClaimTransaction_RecoveryReserve.ClaimNumber,
			ODS_ClaimTransaction_RecoveryReserve.ProductCode


	create index idx1 on #ODS_ClaimTransaction_RecoveryReserve(ClaimNumber);

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 5 -- Gather NetPaid From ODS ClaimTransaction Table
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #ODS_ClaimTransaction_NetPaid

	select 

		ODS_ClaimTransaction_NetPaid.claimsk,
		ODS_ClaimTransaction_NetPaid.ClaimNumber,
		case 
			when ODS_ClaimTransaction_NetPaid.ProductCode like 'NI%' THEN SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtLcl*-1,0))
			else SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtrpt*-1,0)) 
		end AS 'ClaimTransactionAmt'

	into #ODS_ClaimTransaction_NetPaid

	from

	(
		select 
			ClaimTransaction.ClaimTransactionSK,
			ClaimTransaction.claimsk,
			Claim.ClaimNumber,
			ClaimTransaction.ClaimSectionSk,
			Product.ProductCode,
			ClaimTransaction.ClaimTransactionTypSK,
			ClaimTransactionType.TransactionTypeCd,
			ClaimTransactionType.TransactionTypeName,
			ClaimTransactionType.TransactionTypeSourceValue,
			ClaimTransaction.ClaimPaymentTypSK,
			ClaimPaymentType.PaymentTypeName,
			ClaimTransaction.ClaimSourceTransactionID,
			ClaimTransaction.ClaimTransactionDt,
			ClaimTransaction.ClaimTransactionAmtLcl,
			ClaimTransaction.ClaimTransactionAmtRpt,
			ClaimTransaction.ClaimTransactionEstimateRecoveryInd,
			ClaimTransaction.ClaimTransactionReInsuranceInd

		from 
			ods.ClaimTransaction ClaimTransaction

			left join ods.ClaimTransactionType ClaimTransactionType
				on ClaimTransaction.ClaimTransactionTypSK=ClaimTransactionType.TransactionTypeSK
				and ClaimTransaction.ClaimTransactionDt<@monthstartdate

			left join ods.ClaimPaymentType ClaimPaymentType
				on ClaimTransaction.ClaimPaymentTypSK=ClaimPaymentType.PaymentTypeSK

			LEFT JOIN ODS.ODS.Claim Claim
				on Claim.claimsk=ClaimTransaction.claimsk
				and Claim.currentrow=1

			-- Join with Policy Information
			LEFT JOIN ODS.ODS.Policy Policy 
				ON claim.PolicySK = Policy.PolicySK 
				AND Policy.CurrentRow='1'

			-- Join with Policy Source
			LEFT JOIN ODS.ODS.Source Source 
				ON Source.SourceKey = Policy.SourceSystemSk 
				AND Policy.CurrentRow='1'

			-- Join with Product Info
			LEFT JOIN ODS.ODS.Product Product 
				ON Product.ProductSK = Policy.ProductSK
				and Product.ProductSource=Source.SourceName
				AND Product.CurrentRow='1'

			join #ODS_DWH_Data ODS_DWH_Data
				on ClaimTransaction.claimsk = ODS_DWH_Data.ClaimKey 

			where 
				ClaimTransaction.CurrentRow=1
				and ClaimTransactionType.TransactionTypeCd in ('PAY','REC')
				
		) as ODS_ClaimTransaction_NetPaid

		group by 
			ODS_ClaimTransaction_NetPaid.claimsk,
			ODS_ClaimTransaction_NetPaid.ClaimNumber,
			ODS_ClaimTransaction_NetPaid.ProductCode


	create index idx1 on #ODS_ClaimTransaction_NetPaid(claimsk);

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 6 -- Gather Fields From Genius ZUMA Table
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #Genius_ZUMA

	select  
		Claimnumber,
		Zuma.MAPORF,	-- Master Reference,
		Zuma.MAMJCD,	-- Master Major Type Code,
		Zuma.MAMAPC,	-- Master Product Code,
		Zuma.MAPNSN,	-- Assured name Code
		Zuma.DateFrom	-- Date Record Updated

	into #Genius_ZUMA

	from 
	DWH.Fact.factclaim c
	left join ODS.ODS.Policy p
	on p.policySK = c.policySK
	left join
	ods.Genius.SI_ZUMA Zuma
	
	--right join #ODS_DWH_Data ODS_DWH_Data
		on Zuma.MAPORF = p.CustomerPolicyNr 
		and Zuma.currentrow=1  AND P.CurrentRow =1
	Where Zuma.DateFrom < @monthstartdate --and Zuma.currentrow=1
	create index idx1 on #Genius_ZUMA(MAPORF);
	create index idx2 on #Genius_ZUMA(MAPNSN);
		create index idx3 on #Genius_ZUMA(Claimnumber);


	-- (262571 rows affected) -- 00:07
	-- (262571 rows affected) -- 02:30

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 7 -- Gather Fields From Genius ZNNA Table (Central Name File)
-- --------------------------------------------------------------------------------------------------------------------------------------------------
	
	drop table if exists #Genius_ZNNA
--	DECLARE @monthstartdate DATETIME; 
---- SET @monthstartdate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);
--SET @monthstartdate = '2024-09-01';

	select c.ClaimNumber,
	Customerpolicynr, Genius_ZUMA.MAMJCD,
		Genius_ZUMA.MAPORF,		-- Policy No
		Genius_ZNNA.NANACD,		-- Name Code
		Genius_ZNNA.Nananm,		-- List Name
		CASE 
			WHEN Genius_ZNNA.Nananm like ('%,%Mrs')  THEN REPLACE(SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm)),'Mrs','')
			WHEN Genius_ZNNA.Nananm like ('%,%Mr')  THEN REPLACE(SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm)),'Mr','')
			WHEN Genius_ZNNA.Nananm like ('%,%Ms')  THEN REPLACE(SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm)),'Ms','')
			WHEN Genius_ZNNA.Nananm like ('%,%Ms.')  THEN REPLACE(SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm)),'Ms.','')
			WHEN Genius_ZNNA.Nananm like ('%,%Mr.')  THEN REPLACE(SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm)),'Mr.','')
			WHEN Genius_ZNNA.Nananm like ('%,%Miss_')  THEN REPLACE(SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm)),'Miss','')
			WHEN Genius_ZNNA.Nananm like ('%,%Mrs%')  THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX('Mrs',Genius_ZNNA.Nananm)+LEN('Mrs')+1,LEN(Genius_ZNNA.Nananm))
			WHEN Genius_ZNNA.Nananm like ('%,%Mr%')  THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX('Mr',Genius_ZNNA.Nananm)+LEN('Mr')+1,LEN(Genius_ZNNA.Nananm))
			WHEN Genius_ZNNA.Nananm like ('%,%Ms%')  THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX('Ms',Genius_ZNNA.Nananm)+LEN('Ms')+1,LEN(Genius_ZNNA.Nananm))
			WHEN Genius_ZNNA.Nananm like ('%,%Ms.%')  THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX('Ms.',Genius_ZNNA.Nananm)+LEN('Ms.')+1,LEN(Genius_ZNNA.Nananm))
			WHEN Genius_ZNNA.Nananm like ('%,%Mr.%')  THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX('Mr.',Genius_ZNNA.Nananm)+LEN('Mr.')+1,LEN(Genius_ZNNA.Nananm))			
			WHEN Genius_ZNNA.Nananm like ('%,%Miss%')  THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX('Miss',Genius_ZNNA.Nananm)+LEN('Miss')+1,LEN(Genius_ZNNA.Nananm))
			WHEN Genius_ZNNA.Nananm like ('%,%') THEN SUBSTRING(Genius_ZNNA.Nananm,CHARINDEX(',',Genius_ZNNA.Nananm)+1,LEN(Genius_ZNNA.Nananm))
		ELSE '' END as [FirstName],
		CASE 
			WHEN Nananm like ('%,%') THEN SUBSTRING(Nananm,1,LEN(SUBSTRING(Nananm,1,CHARINDEX(',',Nananm)-1)))
		ELSE SUBSTRING(Nananm,CHARINDEX(',',Nananm)+1,LEN(Nananm)) END as [Surname / Company Name]


	into #Genius_ZNNA

	from 
	DWH.fact.factclaim c
		left join
		ODS.ODS.Policy p
		on c.PolicySK=p.PolicySK and p.CurrentRow='1'
	
		left join 
		 ODS.Genius.SI_ZUMA Genius_ZUMA
		 on Genius_ZUMA.MAPORF = p.CustomerPolicyNr and Genius_ZUMA.CurrentRow='1'
		
		left join
		ods.Genius.SI_ZNNA Genius_ZNNA
	
		On Genius_ZNNA.NANACD = Genius_ZUMA.MAPNSN  
		and Genius_ZNNA.currentrow=1
--		and	Genius_ZNNA.DateFrom < @monthstartdate
	--where c.claimnumber in ('')
	group by
		c.ClaimNumber,
		Customerpolicynr,Genius_ZUMA.MAMJCD,
		Genius_ZUMA.MAPORF,		-- Policy No
		Genius_ZNNA.NANACD,		-- Name Code
		Genius_ZNNA.Nananm		-- List Name

	create index idx1 on #Genius_ZNNA(MAPORF);

	-- (208203 rows affected) -- 00:06
	-- (208203 rows affected) -- 00:55

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 8 -- Gather Fields From Genius ZKFB Table (Claim Section)
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #Genius_ZKFB

--DECLARE @monthstartdate DATETIME; 
---- SET @monthstartdate = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);
--SET @monthstartdate = '2024-09-01';	
	select distinct
	
		C.ClaimNumber, -- Our Claim Trans Ref
		CASE 
			WHEN min([Injury Element]) over (partition by c.ClaimNumber) = 1 then 'Yes' 
			else 'No' 
		End as [Injury Element]
		--min(Genius_ZKFB.[Injury Element]) as [Injury Element],
		--max(Genius_ZKFB.DateFrom) as DateFrom
		into #Genius_ZKFB

		from DWH.fact.factclaim c 
		left join
		(
			Select 
	
				Genius_ZKFB.FBFANO, -- Claim Master Code
				Genius_ZKG0.G0G0OR, -- Our Claim Trans Ref
				Genius_ZKFB.fbfbtl, -- Claim Section Title
				CASE 
					WHEN left(cast(Genius_ZKFB.fbfbtl as varchar(255)),1) = '2' THEN 1
					WHEN left(cast(Genius_ZKFB.FBFBTL as varchar(255)),1) = 'P' THEN 2 
					else 2 
				End as [Injury Element],
				Genius_ZKFB.DateFrom

				FROM ODS.Genius.SI_ZKG0 Genius_ZKG0	
		
				right join #ODS_DWH_Data ODS_DWH_Data
					on Genius_ZKG0.G0G0OR = ODS_DWH_Data.ClaimNumber 
					and Genius_ZKG0.currentrow=1
									
				left join ODS.Genius.SI_ZKFB Genius_ZKFB
					on Genius_ZKG0.G0FANO = Genius_ZKFB.FBFANO 
					and Genius_ZKG0.G0FBCD = Genius_ZKFB.FBFBCD 
					and Genius_ZKFB.currentrow =1
				--where Genius_ZKFB.DateFrom < @monthstartdate
			) as Genius_ZKFB on c.claimnumber = Genius_ZKFB.G0G0OR
			--where claimnumber in ('')
			group by ClaimNumber,[Injury Element]
	
	create index idx1 on #Genius_ZKFB(ClaimNumber);
	-- (254835 rows affected) -- 00:10
	-- (254835 rows affected) -- 00:18


-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 9 -- Gather Field From Genius LKLL Table (K9 Screen) - Claim Reopened 
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #Genius_LKLL_Claim_Reopened

	select 
		Genius_ZKFA.FAFAMR as [Claim_Master_Ref],
		Genius_LKLL.LGTYCD,
		CASE 
			WHEN Genius_LKLL.LGTYCD = 'REO' THEN max(Genius_LKLL.LGDATE) 
		End as [Claim Reopened Date]

		into #Genius_LKLL_Claim_Reopened
					
		FROM ODS.Genius.SI_LKLL Genius_LKLL

		join ODS.Genius.SI_ZKFA Genius_ZKFA 
			on Genius_LKLL.LGFANO=Genius_ZKFA.FAFANO
			and Genius_ZKFA.CurrentRow=1

		join #ODS_DWH_Data ODS_DWH_Data
			on Genius_ZKFA.FAFAMR = ODS_DWH_Data.ClaimNumber 
							
		where 
			Genius_LKLL.LGTYCD in ('REO')
			and Genius_LKLL.CurrentRow=1 and
			Genius_LKLL.DateFrom < @monthstartdate					
		group by 
			Genius_ZKFA.FAFAMR,
			Genius_LKLL.LGTYCD
			;

		create index idx1 on #Genius_LKLL_Claim_Reopened([Claim_Master_Ref]);
					
		-- (6358 rows affected) -- 00:01
		-- (6358 rows affected) -- 00:21

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 10 -- Gather Fields From Genius LKLL Table (K9 Screen) - Most Recent date on KV
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #Genius_LKLL_Recent_KV_Date

	select 
		Genius_ZKFA.FAFAMR as [Claim_Master_Ref],
		Genius_LKLL.LGTYCD,
		CASE 
			WHEN Genius_LKLL.LGTYCD = 'RES' THEN max(Genius_LKLL.LGDATE) 
		End as [Most Recent date on KV]

		into #Genius_LKLL_Recent_KV_Date
					
		FROM ODS.Genius.SI_LKLL Genius_LKLL

		join ODS.Genius.SI_ZKFA Genius_ZKFA 
			on Genius_LKLL.LGFANO=Genius_ZKFA.FAFANO
			and Genius_ZKFA.CurrentRow=1

		join #ODS_DWH_Data ODS_DWH_Data
			on Genius_ZKFA.FAFAMR = ODS_DWH_Data.ClaimNumber 
							
		where 
			Genius_LKLL.LGTYCD in ('RES')
			and Genius_LKLL.CurrentRow=1
			and Genius_LKLL.DateFrom < @monthstartdate

								
		group by 
			Genius_ZKFA.FAFAMR,
			Genius_LKLL.LGTYCD
			;

		create index idx1 on #Genius_LKLL_Recent_KV_Date([Claim_Master_Ref]);

		-- (4927 rows affected) -- 00:00
		-- (4927 rows affected) -- 00:00


-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 11 -- Gather Fields From Genius ZKG0 Table (Claim Transaction Header)
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #Genius_ZKG0

	select distinct
	
		Genius_ZKG0.ClaimNumber,
		Case when min(Genius_ZKG0.Priority_OC) over (partition by Claimnumber) = '1' then 'GBP' else 'EUR' End as [Or Cur on K9],
		Case when min(Genius_ZKG0.Priority_AC) over (partition by Claimnumber) = '1' then 'GBP' else 'EUR' End as [AC Cur on K9]

		into #Genius_ZKG0	
		
		from 
		(
		
			select
				Genius_ZKFA.FAFAMR as ClaimNumber,
				Genius_ZKG0.G0FANO,--Claim Master Code
				Genius_ZKG0.G0FBCD,--Claim Section code
				Genius_ZKG0.G0F3CD,--Claim Trs HDR Code
				Genius_ZKG0.G0F3OC, -- as 'Org Cur on K9',
				Genius_ZKG0.G0F3AC,--A/C cur on K9
				CASE WHEN Genius_ZKG0.G0F3OC = 'GBP' THEN 1 else 2 End as 'Priority_OC',
				CASE WHEN Genius_ZKG0.G0F3AC = 'GBP' THEN 1 else 2 End as 'Priority_AC'

				FROM [ODS].[Genius].[SI_ZKG0] Genius_ZKG0
					
				join  [ODS].[Genius].[SI_ZKFA] Genius_ZKFA
					ON Genius_ZKG0.G0FANO = Genius_ZKFA.FAFANO
					and Genius_ZKG0.CurrentRow=1
					and Genius_ZKFA.CurrentRow=1

				join #ODS_DWH_Data ODS_DWH_Data
					on Genius_ZKFA.FAFAMR = ODS_DWH_Data.ClaimNumber 

				Where 	Genius_ZKG0.datefrom < @monthstartdate

				group by 
					Genius_ZKFA.FAFAMR,
					Genius_ZKG0.G0FANO,
					Genius_ZKG0.G0FBCD,
					Genius_ZKG0.G0F3CD,
					Genius_ZKG0.G0F3OC,
					Genius_ZKG0.G0F3AC
		
		) as Genius_ZKG0
		group by Genius_ZKG0.ClaimNumber,Priority_OC,Priority_AC

		create index idx1 on #Genius_ZKG0([ClaimNumber]);

		-- (254835 rows affected) -- 00:03
		-- (254835 rows affected) -- 00:37

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 12 -- Gather Fields From ODS Transaction Table for Gross OS1 (Claim Section Summary)
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #ODS_Gross_OS1

	Select distinct
ClaimNumber,
SUM(ISNULL(ODS_claimtransaction.claimtransactionAmtrpt,0)) over (partition by ClaimNumber) as [Gross OS1]
into #ODS_Gross_OS1
from 
ODS.Ods.Claim ODS_Claim
left join
ODS.ODS.ClaimTransaction ODS_Claimtransaction
on ODS_Claim.ClaimSK = ODS_Claimtransaction.ClaimSK and ODS_Claim.CurrentRow = '1' 
and ODS_Claimtransaction.CurrentRow = '1'
where 
--claimnumber = '' and
ClaimTransactionTypSK = '2' and ClaimTransactionEstimateRecoveryInd=0 and ClaimTransactionReInsuranceInd = 0
and ClaimTransactionDt < @monthstartdate --and ClaimReportDt >= '2011-01-01' 
and ClaimSourceTransactionID <> 0
group by ODS_Claimtransaction.Claimsk,ClaimTransactionSK,ClaimNumber,claimtransactionAmtrpt--,ODS_Claimtransaction.ClaimTransactionDt
	create index idx1 on #ODS_Gross_OS1([ClaimNumber]);

	-- (254773 rows affected) -- 00:04
	-- (254773 rows affected) -- 00:21

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 13 -- Gather Fields From ODS Table (Claim Transaction Amounts) -- Net Paid
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #ODS_NetPaid
select distinct
 
		ODS_ClaimTransaction_NetPaid.claimsk,
		ODS_ClaimTransaction_NetPaid.ClaimNumber,
		case 
			--when AGA137 like 'N%' THEN SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtLcl*-1,0)   OVER (PARTITION BY ODS_ClaimTransaction_NetPaid.ClaimNumber)
			--When AGA137 not like 'N%' or AGA137 is NULL THEN SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtRpt*-1,0))   OVER (PARTITION BY ODS_ClaimTransaction_NetPaid.ClaimNumber)
			--When  claimnumber like ('%/%/%') then SUM( isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtRpt*-1,0)) OVER (PArtITION BY ODS_ClaimTransaction_NetPaid.ClaimNumber) ELSE 0 

			when ODS_ClaimTransaction_NetPaid.AGA137 like 'N%' THEN SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtLcl*-1,0)) over (partition by ODS_ClaimTransaction_NetPaid.ClaimNumber)
			WHEN ODS_ClaimTransaction_NetPaid.AGA137 not like 'N%' or ODS_ClaimTransaction_NetPaid.AGA137 is NULL THEN SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtrpt*-1,0)) over (partition by ODS_ClaimTransaction_NetPaid.ClaimNumber)
			WHEN claimnumber like ('%/%/%') then SUM( isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtRpt*-1,0)) OVER (PArtITION BY ODS_ClaimTransaction_NetPaid.ClaimNumber)
			else 0
		end AS 'Net Paid',
		case 
			when ODS_ClaimTransaction_NetPaid.ProductCode like 'NI%' and ClaimSourceTransactionID <> '0' and ClaimTransactionEstimateRecoveryInd = '0' and ClaimTransactionReInsuranceInd = '0' THEN SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtLcl*-1,0)) over (partition by ODS_ClaimTransaction_NetPaid.ClaimNumber)
			else SUM(isnull(ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtrpt*-1,0)) over (partition by ODS_ClaimTransaction_NetPaid.ClaimNumber)
		end AS 'Gross Paid'
 
 
	into #ODS_NetPaid
 
	from
 
	(
		select 
			ClaimTransaction.ClaimTransactionSK,
			ClaimTransaction.claimsk,
			Claim.ClaimNumber,
			ClaimTransaction.ClaimSectionSk,
			Product.ProductCode,AGA137,
			ClaimTransaction.ClaimTransactionTypSK,
			ClaimTransactionType.TransactionTypeCd,
			ClaimTransactionType.TransactionTypeName,
			ClaimTransactionType.TransactionTypeSourceValue,
			ClaimTransaction.ClaimPaymentTypSK,
			ClaimPaymentType.PaymentTypeName,
			ClaimTransaction.ClaimSourceTransactionID,
			ClaimTransaction.ClaimTransactionDt,
			ClaimTransaction.ClaimTransactionAmtLcl,
			ClaimTransaction.ClaimTransactionAmtRpt,
			ClaimTransaction.ClaimTransactionEstimateRecoveryInd,
			ClaimTransaction.ClaimTransactionReInsuranceInd
 
		from 
			ods.ClaimTransaction ClaimTransaction
 
			left join ods.ClaimTransactionType ClaimTransactionType
				on ClaimTransaction.ClaimTransactionTypSK=ClaimTransactionType.TransactionTypeSK
				and ClaimTransaction.ClaimTransactionDt<@monthstartdate
			
			left join ods.ClaimPaymentType ClaimPaymentType
				on ClaimTransaction.ClaimPaymentTypSK=ClaimPaymentType.PaymentTypeSK
 
			LEFT JOIN ODS.ODS.Claim Claim
				on Claim.claimsk=ClaimTransaction.claimsk
				and Claim.currentrow=1
			Left join ODS.GEnius.SI_ZKFA ZKFA
				on ZKFA.FAFAMR = Claim.ClaimNumber and ZKFA.CurrentRow = 1
				left join ODS.genius.SI_ZNAG ZNAG
				on ZKFA.FAFALB = ZNAG.AGNACD  and ZNAG.CurrentRow = 1

			-- Join with Policy Information
			LEFT JOIN ODS.ODS.Policy Policy 
				ON claim.PolicySK = Policy.PolicySK 
				AND Policy.CurrentRow='1'
 
			-- Join with Policy Source
			LEFT JOIN ODS.ODS.Source Source 
				ON Source.SourceKey = Policy.SourceSystemSk 
				AND Policy.CurrentRow='1'
 
			-- Join with Product Info
			LEFT JOIN ODS.ODS.Product Product 
				ON Product.ProductSK = Policy.ProductSK
				and Product.ProductSource=Source.SourceName
				AND Product.CurrentRow='1'
 
			--join #ODS_DWH_Data ODS_DWH_Data
			--	on ClaimTransaction.claimsk = ODS_DWH_Data.ClaimKey
 
			where 
				ClaimTransaction.CurrentRow=1
				and ClaimTransactionType.TransactionTypeCd in ('PAY','REC') and year(ClaimReportDt) >= '2000' 
				and Claim.BatchNumber <> '33333333' and ClaimTransaction.BatchNumber <> '22222222' 
				and ClaimTransaction.BatchNumber <> '33333333' 
				--ClaimSourceTransactionID <> '0' 
				and ClaimTransactionEstimateRecoveryInd = '0'
		) as ODS_ClaimTransaction_NetPaid
  --where ClaimNumber in ('')

  
		--group by 
		--	ODS_ClaimTransaction_NetPaid.claimsk,
		--	ODS_ClaimTransaction_NetPaid.ClaimNumber,
		--	ODS_ClaimTransaction_NetPaid.ProductCode,AGA137,ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtLcl,ODS_ClaimTransaction_NetPaid.ClaimTransactionAmtRpt,ClaimSourceTransactionID
		--	,ClaimTransactionEstimateRecoveryInd,ClaimTransactionReInsuranceInd

	 create index idx1 on #ODS_NetPaid(ClaimNumber);


	-- (202527 rows affected) -- 00:51


-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 14 -- Gather Fields From ODS Transactions Table (Claim Transaction Amounts) -- Gross Paid
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #ODS_GrossPaid


	
				Select
				distinct Claim.ClaimSK,
				Claim.ClaimNumber,
				Product.ProductCode ,AGA137,
	
			case 
						when AGA137 like 'N%' THEN SUM(CASE WHEN ClaimTransactionType.transactiontypecode in ('PAY','REC') and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ClaimTransaction.claimtransactionamountlocal,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
						WHEN AGA137 not like 'N%' or AGA137 is NULL Then SUM(CASE WHEN ClaimTransactionType.transactiontypecode in ('PAY','REC') and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ClaimTransaction.claimtransactionamountreporting,0) ELSE 0 END)  OVER (PARTITION BY Claim.claimsk)
						WHEN claimnumber like ('%/%/%') then SUM(CASE WHEN ClaimTransactionType.transactiontypecode in ('PAY','REC') THEN isnull(ClaimTransaction.claimtransactionamountreporting*-1,0)ELSE 0 END) OVER (PArtITION BY Claim.Claimsk)
					else 0

					end AS 'Gross Paid'

			into #ODS_GrossPaid
	
	
			From
				ODS.ODS.Claim Claim 
			LEFT JOIN DWH.Fact.FactClaimTransaction  ClaimTransaction
					ON ClaimTransaction.ClaimSK = claim.Claimsk and claim.CurrentRow = 1
		Left join ODS.GEnius.SI_ZKFA ZKFA
				on ZKFA.FAFAMR = Claim.ClaimNumber and ZKFA.CurrentRow = 1
				left join ODS.genius.SI_ZNAG ZNAG
				on ZKFA.FAFALB = ZNAG.AGNACD  and ZNAG.CurrentRow = 1

				LEFT JOIN ODS.ODS.claimtransaction ODS_ClaimTransaction
					on ClaimTransaction.ClaimTransactionKey=ODS_ClaimTransaction.ClaimTransactionsk
					and ClaimTransaction.ClaimSK=ODS_ClaimTransaction.ClaimSK
					and ODS_ClaimTransaction.CurrentRow=1
 
				-- Join with Transaction Type Table to derive the type of each transaction
				LEFT JOIN dwh.dim.DimClaimTransactionType ClaimTransactionType 
					ON ClaimTransaction.TransactionTypeSK =ClaimTransactionType.TransactionTypeKey 
					-- Join with Policy Information
				LEFT JOIN ODS.ODS.Policy Policy 
					ON claim.PolicySK = Policy.PolicySK 
					AND Policy.CurrentRow='1'
 
				-- Join with Policy Source
				LEFT JOIN ODS.ODS.Source Source 
					ON Source.SourceKey = Policy.SourceSystemSk 
					AND Policy.CurrentRow='1'
					-- Join with Product Info
				LEFT JOIN ODS.ODS.Product Product 
					ON Product.ProductSK = Policy.ProductSK
					and Product.ProductSource=Source.SourceName
					AND Product.CurrentRow='1'
				WHERE
	

		YEAR(claim.ClaimReportDt) >= '2000'  and claim.BatchNumber <> '22222222' and ODS_ClaimTransaction.BatchNumber <> '22222222' 
		and ODS_ClaimTransaction.BatchNumber <> '33333333'
		 --and NOT(claim.SourceName = 'Benchmark' AND claim.ClaimDescription LIKE '%liability%')
	 and ClaimTransaction.TransactionDate<=Eomonth(DATEADD(month,-1,GETDATE()))
	 --and claim.ClassOfBusinessSK is not null

	create index idx1 on #ODS_GrossPaid([ClaimNumber]);

	-- (492926 rows affected) -- 00:08
	-- (492932 rows affected -- 00:23
	-- (202472 rows affected) -- 00:09

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 15 -- Gather Fields From Genius ZKG3 Table (Claim Transaction Amounts) -- Recovery Reserve
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #Genius_ZKG3_Recovery_Reserve
	
	Select 
		distinct 
		Genius_ZKG3.G3FANO, --Claim Master Code
		Genius_ZKG0.G0G0OR as [ClaimNumber], --Claim Number
		Sum(floor((isnull(Genius_ZKG3.G3F8MB,0)))) over (partition by Genius_ZKG3.G3fano)*(-1) as 'Recovery Reserve',
		max(Genius_ZKG3.DateFrom) as DateFrom
		
		into #Genius_ZKG3_Recovery_Reserve
	
		FROM ODS.Genius.SI_ZKG3 Genius_ZKG3
						
		left join ODS.GEnius.SI_ZKG0 Genius_ZKG0
			on Genius_ZKG3.G3FANO = Genius_ZKG0.G0fano 
			and Genius_ZKG3.CurrentRow = 1
			and Genius_ZKG0.CurrentRow = 1
			and Genius_ZKG0.G0FBCD = Genius_ZKG3.g3fbcd
			and CONCAT(Genius_ZKG0.G0CTHA,Genius_ZKG0.G0G0P2) not in (32,33) 
			and Genius_ZKG0.G0CTHA not in (2,0)


		join #ODS_DWH_Data ODS_DWH_Data
			on Genius_ZKG0.G0G0OR = ODS_DWH_Data.ClaimNumber 
		
		where 
			Genius_ZKG0.G0G0OR <> ' ' 
			and	Genius_ZKG3.G3AMCD = 'O/S' --Checking if Incas Amount Code =O/S
			and Genius_ZKG3.G3G3M1 = 'REC' --Checking if ClTrAmt Mvmt Type 1 code = REC 
			and Genius_ZKG3.G3G3PO = 0	   --and CltrAmt Payment or O/S = 0
						
		group by 
			Genius_ZKG3.G3FANO,
			Genius_ZKG0.G0G0OR,
			Genius_ZKG3.JOSEQN,
			Genius_ZKG3.G3F8MB,
			Genius_ZKG3.G3AMCD,
			Genius_ZKG3.G3G3M1,
			Genius_ZKG3.G3FBCD

	create index idx1 on #Genius_ZKG3_Recovery_Reserve([ClaimNumber]);

	-- (62837 rows affected) -- 00:16
	-- (62837 rows affected) -- 00:16

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 16 -- Gather Fields From ODS Treansaction Table (Claim Transaction Amounts) -- Net O/S
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #ODS_NetOS
	Select distinct
			ClaimNumber,product.ProductCode as 'Product',
			case 
						when AGA137 like 'N%' THEN SUM(CASE WHEN ClaimTransactionType.TransactionTypeCd in ('O/S') and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0 THEN isnull(ODS_Claimtransaction.ClaimTransactionAmtLcl,0) ELSE 0 END)  OVER (PARTITION BY ODS_Claim.claimsk)
						WHEN AGA137 not like 'N%' or AGA137 is NULL then SUM(CASE WHEN ClaimTransactionType.TransactionTypeCd in ('O/S') and ODS_ClaimTransaction.ClaimTransactionEstimateRecoveryInd=0  THEN isnull(ODS_Claimtransaction.ClaimTransactionAmtRpt,0) ELSE  0 END)  OVER (PARTITION BY ODS_Claim.claimsk)
						WHEN ClaimNumber like ('%/%/%') then SUM(CASE WHEN ClaimTransactionType.TransactionTypeCd in ('O/S')  THEN isnull(ODS_Claimtransaction.ClaimTransactionAmtRpt,0) ELSE  0 END)  OVER (PARTITION BY ODS_Claim.claimsk)
					end AS 'NetOS'
					into #ODS_NetOS
			--SUM(ODS_claimtransaction.claimtransactionAmtrpt) over (partition by ClaimNumber) as [Net O/S]
			from 
			ODS.Ods.Claim ODS_Claim 
			LEFT JOIN DWH.Fact.FactClaimTransaction  ClaimTransaction
			ON ClaimTransaction.ClaimSK = ODS_Claim.Claimsk and ODS_Claim.CurrentRow = 1 
			 Left join ODS.GEnius.SI_ZKFA ZKFA
							on ZKFA.FAFAMR = ODS_Claim.ClaimNumber and ZKFA.CurrentRow = 1
							left join ODS.genius.SI_ZNAG ZNAG
							on ZKFA.FAFALB = ZNAG.AGNACD  and ZNAG.CurrentRow = 1

			left join
			ODS.ODS.ClaimTransaction ODS_Claimtransaction
			on ODS_Claim.ClaimSK = ODS_Claimtransaction.ClaimSK --and ODS_Claimtransaction.ClaimTransactionSK = claimtransaction.ClaimTransactionKey
				-- Join with Transaction Type Table to derive the type of each transaction
				LEFT JOIN ODS.ODS.ClaimTransactionType ClaimTransactionType 
					ON ODS_Claimtransaction.ClaimTransactionTypSK =ClaimTransactionType.TransactionTypeSK 
					--and ODS_Claim.ClaimSK = ClaimTransactionType.claimsk
					and ODS_ClaimTransaction.CurrentRow=1
 
					LEFT JOIN ODS.ODS.Policy Policy 
					ON ODS_Claim.PolicySK = Policy.PolicySK 
					AND Policy.CurrentRow='1'
 
				-- Join with Policy Source
				LEFT JOIN ODS.ODS.Source Source 
					ON Source.SourceKey = Policy.SourceSystemSk 
					AND Policy.CurrentRow='1'
					-- Join with Product Info
				LEFT JOIN ODS.ODS.Product Product 
					ON Product.ProductSK = Policy.ProductSK
					and Product.ProductSource=Source.SourceName
					AND Product.CurrentRow='1' --and product.ProductCode is not null
	

			where 
			--claimnumber = '' and
			ClaimTransactionTypSK  = 2 
			--and ODS_Claimtransaction.ClaimSourceTransactionID <> 0 
			and ClaimTransactionEstimateRecoveryInd=0 and
			--and ClaimTransactionReInsuranceInd=0 
			 year(claimreportdt) >= '2000'
			and ClaimTransactionDt <@monthstartdate 
			and  ODS_Claim.BatchNumber <> '22222222' and ODS_Claim.BatchNumber <> '33333333' and ODS_ClaimTransaction.BatchNumber <> '22222222' 
			and ODS_ClaimTransaction.BatchNumber <> '33333333' and ClaimTransactionSK is not null and ClaimTransactionKey is not null
			group by ODS_Claim.claimsk,ClaimNumber,ClaimTransactionSK,TransactionTypeCd,ClaimTransactionEstimateRecoveryInd,ProductCode,AGA137,ClaimNumber,claimtransactionAmtrpt,ClaimTransactionAmtLcl
			,ODS_claimtransaction.ClaimSourceTransactionID
--
	create index idx1 on #ODS_NetOS([ClaimNumber]);

	-- (254647 rows affected) -- 00:53
	-- (254647 rows affected) -- 00:34


--Net Remaining Trans	
Drop table if exists #Net_remaining_trans
Select ClaimNumber, SUM(G3F8MB)  as 'Net remaining Trans'
into #Net_remaining_trans
From DWH.FACt.FACtCLaim c left join 
(
Select G0G0OR,(G3F8MB),G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT 
from ODS.Genius.SI_ZKG3 g3
right join ODS.Genius.SI_ZKG0 o
on o.G0FANO = g3.G3FANO
--and g3.G3F3CD = o.G0F3CD
--and g3.G3FBCD = o.G0FBCD
--where G0G0OR = ''
where G3G3M2 = 'REX' and G3G3PO = 0 --and G3G3M1 = 'REC'
and G3OLDT < '2025-01-01' and g3.CurrentRow = 1 and o.CurrentRow =1 --and G0G0OR in ('')
group by G0G0OR,G3F8MB,G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT) as j on j.G0G0OR = c.Claimnumber
group by ClaimNumber
--where G0G0OR in ('')
create index idx1 on #Net_remaining_trans([ClaimNumber]);



--Gross Remaining Trans	
Drop table if exists #Gross_remaining_trans
Select ClaimNumber, SUM(G3F8MB)  as 'Gross remaining Trans'
into #Gross_remaining_trans
From DWH.FACt.FACtCLaim c left join 
(
Select G0G0OR,(G3F8MB),G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT 
from ODS.Genius.SI_ZKG3 g3
right join ODS.Genius.SI_ZKG0 o
on o.G0FANO = g3.G3FANO
--and g3.G3F3CD = o.G0F3CD
--and g3.G3FBCD = o.G0FBCD
--where G0G0OR = ''
where G3G3M2 = 'REX' and G3G3PO = 0 --and G3G3M1 = 'REC'
and G3OLDT < '2025-01-01' and g3.CurrentRow = 1 and o.CurrentRow =1 --and G0G0OR in ('')
group by G0G0OR,G3F8MB,G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT) as j on j.G0G0OR = c.Claimnumber
group by ClaimNumber
--where G0G0OR in ('')
create index idx1 on #Gross_remaining_trans([ClaimNumber]);




--Gross Exp Trans
Drop table if exists #gross_Exp_trans
Select  distinct ClaimNumber, SUM(G3F8MB)  as 'Gross Expec Trans'
into #gross_Exp_trans
From ODS.ODS.Claim c left join
(
Select G0G0OR,(G3F8MB),G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT 
from ODS.Genius.SI_ZKG3 g3
right join ODS.Genius.SI_ZKG0 o
on o.G0FANO = g3.G3FANO
--and g3.G3F3CD = o.G0F3CD
--and g3.G3FBCD = o.G0FBCD
--where G0G0OR = ''
where G3G3M2 = 'REX' and G3G3PO = 0 and G3G3M1 = 'REC'
and G3OLDT < '2025-01-01' and g3.CurrentRow = 1 and o.CurrentRow =1 --and G0G0OR in ('')
group by G0G0OR,G3F8MB,G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT) as j on j.G0G0OR = c.Claimnumber and c.currentrow =1
--where G0G0OR in ('')
group by ClaimNumber
create index idx1 on #Gross_exp_trans([ClaimNumber]);



--Net Exp Trans
Drop Table if exists #Net_Exp_trans
Select  distinct ClaimNumber, SUM(G3F8MB)  as 'Net Expec Trans'
into #Net_Exp_trans
From ODS.ODS.Claim c left join
(
Select G0G0OR,(G3F8MB),G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT 
from ODS.Genius.SI_ZKG3 g3
right join ODS.Genius.SI_ZKG0 o
on o.G0FANO = g3.G3FANO
--and g3.G3F3CD = o.G0F3CD
--and g3.G3FBCD = o.G0FBCD
where G3G3M2 = 'REX' and G3G3PO = 0 and G3G3M1 = 'REC'
and G3OLDT < '2025-01-01' and g3.CurrentRow = 1 and o.CurrentRow =1 --and G0G0OR in ('')
group by G0G0OR,G3F8MB,G3AMCD,G3G3M1,G3G3M2,G3G3PO,G3OLDT) as j on j.G0G0OR = c.Claimnumber and c.currentrow =1
--where G0G0OR in ('')
group by ClaimNumber
create index idx1 on #Net_exp_trans([ClaimNumber]);

--Gross Recoveries Recd
Drop table if exists #Gross_Recoveries_Recd
Select distinct c.ClaimNumber ,SUM(Transactions) over (partition by c.ClaimNumber) as 'Recovery Received'
into #Gross_Recoveries_Recd
from DWH.fact.factclaim c left join (
Select G0G0OR as 'ClaimNumber',G3G3PO as 'ClmTrs Amt Payment or O/S',G3G3M1 as 'ClmTrs Movement Type Code 1',G3G3M2 as 'ClmTrs Movement Type Code 2',
G0F3MN as 'ClmTrs Hdr RI Master No',G3oldt as 'GGN Oustanding Loss Date',Isnull(G3F8MB,0)*-1 as 'Transactions' from Genius.SI_ZKG0 o 
left join Genius.SI_ZkG3 g
on o.G0FANO = g.G3FANo
and o.G0FBCD =g.G3FBCD
and o.G0F3CD = g.G3FBCD
and g.currentrow = '1' and o.currentrow ='1'
where --G0G0OR in ('') and 
 G3oldt < '2025-01-01'
and g3G3M1 in ('REC') and G3G3M2 <> 'VAT'
and G3G3PO = '1' and G0CTHA <> '2'
and G0F3MN = ' '
--and G3AMCD in ('FEE','CLM')
group by G0G0OR,G3G3PO,G3G3M1,G3G3M2,G0F3MN,G3oldt,G3F8MB ) as k on k.ClaimNumber = c.ClaimNumber 
--where c.claimnumber in ('')
group by c.ClaimNumber,Transactions,k.[GGN Oustanding Loss Date]

create index idx1 on #Gross_Recoveries_Recd([ClaimNumber]);

------------------------------------------------------------------------Motor Injury Element--------------------------------------------------------------------------------------------
Drop table if exists #Motor_injury_element
Select ClaimNumber,N_CLAIM_NUMBER,N_POLICY,C_LINE_TYPE,
case when C_LINE_TYPE is null then 'N' else 'Y' End as 'Motor injury Element'
into #Motor_injury_element
from ODS.ODS.Claim c
left join (
select distinct
a.N_CLAIM_NUMBER,
a.N_POLICY,
b.c_line_type
from ODS.ccs.si_claim a
left join ODS.ccs.si_line b on b.n_claim_id=a.n_claim_id
where (left(a.N_POLICY,3)='mfn' or left(a.N_POLICY,3)='svn') and b.c_line_type='tppi' ) as t on t.N_CLAIM_NUMBER = c.claimnumber
--where ClaimNumber in ('') or N_CLAIM_NUMBER in ('')
group by ClaimNumber,N_CLAIM_NUMBER,N_POLICY,N_CLAIM_NUMBER,C_LINE_TYPE

create index idx1 on #Motor_injury_element([ClaimNumber]);

--County
Drop table if exists #county
Select Claim.ClaimNumber,County into #county 
from DWH.fact.factclaim Claim right join
(
Select FAFAMR as 'ClaimNumber',AAAADS as 'County',FAFAAC as 'Claim Area Code',FAFA1A as '1st Area Code' 

--from DWH.Fact.FactClaim c left join 
from ODS.Genius.SI_ZKFA f-- on c.ClaimNumber = f.FAFAMR 
left join ODS.Genius.SI_ZHAA h on h.AAAACD = f.FAFA1A 
and f.currentrow =1 and h.CurrentRow =1 
and h.datefrom <'2025-01-01' --and f.datefrom < '2024-11-01' 
--where c.CurrentRow = 1
--where FAFAMR in ('')
--and year(ClaimReportDt) > '2000' --and c.ClaimSettlementDt < '2024-11-01'
group by FAFAMR,AAAADS,FAFAAC,FAFA1A) as County on County.ClaimNumber = Claim.ClaimNumber
--Where Claim.ClaimNumber in ('')
group by Claim.ClaimNumber,County
create index idx1 on #county([ClaimNumber]);


-------------------Gross recoveries OS-----------------------------------------------------------------

drop table if exists #Gross_Recoveries_OS
SELECT  distinct
c.ClaimNumber,
--sum(isnull(t.G3F8MB,0)) over (partition by c.claimnumber) AS Gross_recoveries_os
sum(isnull(t.G3F8MB,0))*-1 AS Gross_recoveries_os
into #Gross_Recoveries_OS
     
FROM 
(select distinct cl.claimnumber from DWH.Fact.FactClaim cl) as c

LEFT JOIN
( select a.fafamr,
zkg.G3F8MB
from
ODS.Genius.SI_ZKFA a
    --on c.ClaimNumber=a.FAFAMR 
    --AND a.CurrentRow='1'  --joining DWH table with Genius tables using claim master ref

LEFT JOIN

ODS.Genius.SI_ZKG0 zk
   on a.FAFANO=zk.G0FANO 
   AND zk.CurrentRow='1'  --claim master code

LEFT JOIN 

ODS.Genius.SI_ZKG3 zkg
   on zk.G0FANO=zkg.G3FANO 
   AND zk.G0FBCD=zkg.G3FBCD 
   AND zk.G0F3CD=zkg.G3F3CD 
   AND zkg.CurrentRow='1'
   
   where a.CurrentRow='1' and zkg.G3G3M1='REC'                          --ClTrAmt Mvmt Type 1 Code
          AND zkg.G3G3PO='0'                        --CltrAmt Payment or O/S?
          AND zkg.g3g3m2!='REX'                     --ClTrAmt Mvmt Type 2 Code
          AND zk.g0f3mn=''                          --ClTrHdr RI Mstr No
		  AND zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))) as t
   on c.ClaimNumber=t.FAFAMR--joining on Claim master code,Claim section code AND Claim Trans hdr code

--WHERE c.ClaimNumber in ('') 
group by c.claimnumber
create index idx1 on #Gross_Recoveries_OS([ClaimNumber]);

----------------------------------------Net recoveries received-------------------------------------------

drop table if exists #Net_recoveries_rcd

SELECT  distinct
c.ClaimNumber,
--sum(isnull(t.G3F8MB,0)) over (partition by c.claimnumber) AS Gross_recoveries_os
sum(isnull(t.G3F8MB,0))*-1 AS Net_recoveries_rcd
into #Net_recoveries_rcd
     
FROM 
(select distinct cl.claimnumber from DWH.Fact.FactClaim cl) as c

LEFT JOIN
( select a.fafamr,
zkg.G3F8MB
from
ODS.Genius.SI_ZKFA a
    --on c.ClaimNumber=a.FAFAMR 
    --AND a.CurrentRow='1'  --joining DWH table with Genius tables using claim master ref

LEFT JOIN

ODS.Genius.SI_ZKG0 zk
   on a.FAFANO=zk.G0FANO 
   AND zk.CurrentRow='1'  --claim master code

LEFT JOIN 

ODS.Genius.SI_ZKG3 zkg
   on zk.G0FANO=zkg.G3FANO 
   AND zk.G0FBCD=zkg.G3FBCD 
   AND zk.G0F3CD=zkg.G3F3CD 
   AND zkg.CurrentRow='1'
   
   where a.CurrentRow='1' 
          and zkg.G3G3PO='1'                         --CltrAmt Payment or O/S?
          AND  zkg.G3G3M1='REC'                      --CltrAmt Mvmt type 2 code
		  AND  zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))) as t
   on c.ClaimNumber=t.FAFAMR--joining on Claim master code,Claim section code AND Claim Trans hdr code

--WHERE c.ClaimNumber in ('') 
group by c.claimnumber
--,t.g3f8mb
create index idx1 on #Net_recoveries_rcd([ClaimNumber]);

------------------------------------------Net recoveries OS-------------------------------------------
drop table if exists #Net_recoveries_os

SELECT  distinct
c.ClaimNumber,
--sum(isnull(t.G3F8MB,0)) over (partition by c.claimnumber) AS Gross_recoveries_os
sum(isnull(t.G3F8MB,0))*-1 AS Net_recoveries_os
into #Net_recoveries_os
     
FROM 
(select distinct cl.claimnumber from DWH.Fact.FactClaim cl) as c

LEFT JOIN
( select a.fafamr,
zkg.G3F8MB
from
ODS.Genius.SI_ZKFA a
    --on c.ClaimNumber=a.FAFAMR 
    --AND a.CurrentRow='1'  --joining DWH table with Genius tables using claim master ref

LEFT JOIN

ODS.Genius.SI_ZKG0 zk
   on a.FAFANO=zk.G0FANO 
   AND zk.CurrentRow='1'  --claim master code

LEFT JOIN 

ODS.Genius.SI_ZKG3 zkg
   on zk.G0FANO=zkg.G3FANO 
   AND zk.G0FBCD=zkg.G3FBCD 
   AND zk.G0F3CD=zkg.G3F3CD 
   AND zkg.CurrentRow='1'
   
   where a.CurrentRow='1' 
         and  zkg.G3G3M1='REC'                          --ClTrAmt Mvmt Type 1 Code
          AND zkg.G3G3PO='0'                        --CltrAmt Payment or O/S?
          AND zkg.g3g3m2!='REX'                     --ClTrAmt Mvmt Type 2 Code
          --AND zk.g0f3mn=''                          --ClTrHdr RI Mstr No
		  AND zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))) as t
   on c.ClaimNumber=t.FAFAMR--joining on Claim master code,Claim section code AND Claim Trans hdr code

--WHERE c.ClaimNumber in ('') 
group by c.claimnumber
--,t.g3f8mb
create index idx1 on #Net_recoveries_os([ClaimNumber]);


----------------------------------------------Gross Exclusions-----------------------------------------
drop table if exists #Gross_exclusions

SELECT  distinct
c.ClaimNumber,
--sum(isnull(t.G3F8MB,0)) over (partition by c.claimnumber) AS Gross_recoveries_os
sum(isnull(t.G3F8MB,0))*-1 AS Gross_exclusions
into #Gross_exclusions
     
FROM 
(select distinct cl.claimnumber from DWH.Fact.FactClaim cl) as c

LEFT JOIN
( select a.fafamr,
zkg.G3F8MB
from
ODS.Genius.SI_ZKFA a
    --on c.ClaimNumber=a.FAFAMR 
    --AND a.CurrentRow='1'  --joining DWH table with Genius tables using claim master ref

LEFT JOIN

ODS.Genius.SI_ZKG0 zk
   on a.FAFANO=zk.G0FANO 
   AND zk.CurrentRow='1'  --claim master code

LEFT JOIN 

ODS.Genius.SI_ZKG3 zkg
   on zk.G0FANO=zkg.G3FANO 
   AND zk.G0FBCD=zkg.G3FBCD 
   AND zk.G0F3CD=zkg.G3F3CD 
   AND zkg.CurrentRow='1'
   
   where a.CurrentRow='1' 
         and zkg.G3G3M1='REC'
          AND (CONCAT(zk.G0CTHA,zk.G0G0P2) = (32)
		  OR   CONCAT(zk.G0CTHA,zk.G0G0P2) = (33))
		  AND (zkg.g3g3m1!='' OR zkg.G3G3M2!='' OR zkg.G3AMCD!='')
		  AND zk.G0F3MN=''
		  AND  zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))) as t
   on c.ClaimNumber=t.FAFAMR--joining on Claim master code,Claim section code AND Claim Trans hdr code

--WHERE c.ClaimNumber in ('') 
group by c.claimnumber
--,t.g3f8mb
create index idx1 on #Gross_exclusions([ClaimNumber]);


--------------------------------Net exclusions---------------------------------------------------------
drop table if exists #Net_exclusions

SELECT  distinct
c.ClaimNumber,
--sum(isnull(t.G3F8MB,0)) over (partition by c.claimnumber) AS Gross_recoveries_os
sum(isnull(t.G3F8MB,0))*-1 AS Net_exclusions
into #Net_exclusions
     
FROM 
(select distinct cl.claimnumber from DWH.Fact.FactClaim cl) as c

LEFT JOIN
( select a.fafamr,
zkg.G3F8MB
from
ODS.Genius.SI_ZKFA a
    --on c.ClaimNumber=a.FAFAMR 
    --AND a.CurrentRow='1'  --joining DWH table with Genius tables using claim master ref

LEFT JOIN

ODS.Genius.SI_ZKG0 zk
   on a.FAFANO=zk.G0FANO 
   AND zk.CurrentRow='1'  --claim master code

LEFT JOIN 

ODS.Genius.SI_ZKG3 zkg
   on zk.G0FANO=zkg.G3FANO 
   AND zk.G0FBCD=zkg.G3FBCD 
   AND zk.G0F3CD=zkg.G3F3CD 
   AND zkg.CurrentRow='1'
   
   where a.CurrentRow='1' 
         and zkg.G3G3M1='REC'
          AND (CONCAT(zk.G0CTHA,zk.G0G0P2) = (32)
		  OR   CONCAT(zk.G0CTHA,zk.G0G0P2) = (33))
		  AND (zkg.g3g3m1!='' OR zkg.G3G3M2!='' OR zkg.G3AMCD!='')
		  --AND zk.G0F3MN=''
		  AND  zkg.G3OLDT<=Eomonth(DATEADD(month,-1,GETDATE()))) as t
   on c.ClaimNumber=t.FAFAMR--joining on Claim master code,Claim section code AND Claim Trans hdr code

--WHERE c.ClaimNumber in ('') 
group by c.claimnumber
--,t.g3f8mb
create index idx1 on #Net_exclusions([ClaimNumber]);


-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 17.1 -- Combine All Temporary Tables Together For Final Query - Part 1/6
-- --------------------------------------------------------------------------------------------------------------------------------------------------


	drop table if exists #OCDCCD_Output_Part1

	select 
		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate] ,
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		Case 
			when ODS_DWH_Data.Claimnumber like ('%/%/%') then ODS_DWH_Data.[Benchmark Insured] 
			else ODS_DWH_Data.[Claimant]
		End as 'Claimant',
		CASE
			WHEN ODS_DWH_Data.ClaimNumber like ('%/%/%') then 'Benchmark' 
			ELSE ODS_DWH_Data.[claimhandlercd]
		END as 'claimhandlercd',
		CASE
			WHEN ODS_DWH_Data.ClaimNumber like ('%/%/%') then 'Benchmark'
			ELSE ODS_DWH_Data.[Team]
		END as 'Team',
		ODS_DWH_Data.[Cause of Claim],
		ISNULL(ODS_Gross_OS.[Gross O/S],0) as 'Gross O/S',
		ODS_Claimostransbreakup.ClaimTransactionAmt as [Net O/S],
		ODS_Recovery_Reserve.rec as [Recovery Reserve],
		ODS_LOI_FEE.[LOI FEE] as  [LOI FEE],
		--ODS_ClaimTransaction_RecoveryReserve.ClaimTransactionAmt as [Recovery Reserve],		
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data_SectionLevel_Peril.ClaimPerilDescription,
		ODS_Claimostransbreakup.Lossreserveamt as [Gross_Recovery_O/S],
		Genius_ZNNA.MAMJCD as [Claim Major Master Code],
		Genius_ZUMA.MAMAPC as [ProductCode],
		Case when ODS_DWH_Data.Claimnumber like ('%/%/%') then ODS_DWH_Data.[Benchmark Insured] else Genius_ZNNA.NANANM End as [Insured],
		Genius_ZNNA.[FirstName],
		Genius_ZNNA.[Surname / Company Name],
		[Injury Element],
		CASE 
			WHEN Genius_LKLL_Claim_Reopened.LGTYCD  is null THEN 'N'
			WHEN Genius_LKLL_Claim_Reopened.LGTYCD = 'REO' THEN 'Y' 
		End as [Claim Last Reopened],
		max(Genius_LKLL_Claim_Reopened.[Claim Reopened Date]) as [Claim Reopened Date],
		Genius_LKLL_Recent_KV_Date.[Most Recent date on KV] as [Most Recent date on KV],
		isnull([Or Cur on K9],'') as 'Or Cur on K9',
		isnull([AC Cur on K9],'') as 'AC Cur on K9',
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]
		--max(Genius_ZKFB.DateFrom) as Genius_ZKFB_DateUpdated,
		--max(Genius_LKLL_Claim_Reopened.DateFrom) as Genius_LKLL_Claim_Reopened_DateUpdated,

	into #OCDCCD_Output_Part1

	from 
		#ODS_DWH_Data ODS_DWH_Data

		left join #ODS_DWH_Data_SectionLevel_Peril ODS_DWH_Data_SectionLevel_Peril
			on ODS_DWH_Data_SectionLevel_Peril.ClaimKey=ODS_DWH_Data.[ClaimKey]

		left join #ODS_Recovery_Reserve ODS_Recovery_Reserve
			on ODS_Recovery_Reserve.ClaimNumber = ODS_DWH_Data.ClaimNumber
		
		left join #ODS_LOI_FEE ODS_LOI_FEE
			on ODS_LOI_FEE.ClaimNumber = ODS_DWH_Data.ClaimNumber


		left join #ODS_Gross_OS ODS_Gross_OS
			on ODS_Gross_OS.ClaimSK = ODS_DWH_Data.ClaimKey

		left join #ODS_Claimostransbreakup ODS_Claimostransbreakup
			on ODS_Claimostransbreakup.ClaimSK=ODS_DWH_Data.[ClaimKey]
			
		--left join #ODS_ClaimTransaction_RecoveryReserve ODS_ClaimTransaction_RecoveryReserve
		--	on ODS_ClaimTransaction_RecoveryReserve.ClaimSK=ODS_DWH_Data.[ClaimKey]

		--left join #ODS_ClaimTransaction_NetPaid ODS_ClaimTransaction_NetPaid
		--	on ODS_ClaimTransaction_NetPaid.ClaimSK=ODS_DWH_Data.[ClaimKey]
		
		left join #Genius_ZUMA Genius_ZUMA
			--on Genius_ZUMA.MAPORF = ODS_DWH_Data.CustomerPolicyNr
			on Genius_ZUMA.ClaimNumber = ODS_DWH_Data.ClaimNumber
		
		left join #Genius_ZNNA Genius_ZNNA
			on Genius_ZNNA.MAPORF=ODS_DWH_Data.CustomerPolicyNr
		
		left join #Genius_ZKFB Genius_ZKFB
			on Genius_ZKFB.ClaimNumber = ODS_DWH_Data.[ClaimNumber]

		left join #Genius_LKLL_Claim_Reopened Genius_LKLL_Claim_Reopened
			on Genius_LKLL_Claim_Reopened.Claim_Master_Ref = ODS_DWH_Data.[ClaimNumber]

		left join #Genius_LKLL_Recent_KV_Date Genius_LKLL_Recent_KV_Date
			on Genius_LKLL_Recent_KV_Date.Claim_Master_Ref = ODS_DWH_Data.[ClaimNumber]

		left join #Genius_ZKG0 Genius_ZKG0
			on Genius_ZKG0.ClaimNumber = ODS_DWH_Data.[ClaimNumber]

	where ODS_DWH_Data.Rank_Claim = 1

	group by 

		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Benchmark Insured],
		ODS_Gross_OS.[Gross O/S],
		ODS_Recovery_Reserve.rec,
		ODS_LOI_FEE.[LOI FEE] ,
		--ODS_ClaimTransaction_RecoveryReserve.ClaimTransactionAmt,
		ODS_Claimostransbreakup.ClaimTransactionAmt,
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data_SectionLevel_Peril.ClaimPerilDescription,
		ODS_Claimostransbreakup.Lossreserveamt,
		Genius_ZNNA.MAMJCD,
		Genius_ZUMA.MAMAPC,
		Genius_ZNNA.NANANM,
		Genius_ZNNA.[FirstName],
		Genius_ZNNA.[Surname / Company Name],
		Genius_ZKFB.[Injury Element],
		Genius_LKLL_Claim_Reopened.LGTYCD,
		Genius_LKLL_Recent_KV_Date.[Most Recent date on KV],
		Genius_ZKG0.[Or Cur on K9],
		Genius_ZKG0.[Ac Cur on K9],
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group],
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]



	create index idx1 on #OCDCCD_Output_Part1([ClaimNumber]);
	-- (287994 rows affected) 02:24
	-- (287994 rows affected) 01:29
	-- (287994 rows affected) 01:36
-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 17.2 -- Combine All Temporary Tables Together For Final Query - Part 2/6
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #OCDCCD_Output_Part2

	select 
		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],
		ODS_DWH_Data.[LOI FEE],
		--ODS_DWH_Data.[Net Paid],		
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		isnull(ODS_DWH_Data.[Claim Reopened Date],'') as 'Claim Reopened Date',
		isnull(ODS_DWH_Data.[Most Recent date on KV],'') as 'Most Recent date on KV',
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],

		Case
		When ODS_DWH_Data.[System] = 'NI Genius' then Isnull(ODS_Gross_OS1.[Gross OS1],0)*-1 else 0 End as 'Gross O/S1',
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]


	into #OCDCCD_Output_Part2

	from 
		#OCDCCD_Output_Part1 ODS_DWH_Data

		left join #ODS_Gross_OS1 ODS_Gross_OS1
			on ODS_Gross_OS1.ClaimNumber = ODS_DWH_Data.[ClaimNumber]

	group by 

		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],
		ODS_DWH_Data.[LOI FEE],
		--ODS_DWH_Data.[Net Paid],				
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],

		ODS_Gross_OS1.[Gross OS1],
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]


	create index idx1 on #OCDCCD_Output_Part2([ClaimNumber]);
	-- (287994 rows affected) -- 00:39
	-- (287994 rows affected) -- 00:04
	-- (287994 rows affected) -- 00:05

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 17.3 -- Combine All Temporary Tables Together For Final Query - Part 3/6
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #OCDCCD_Output_Part3

	select 
		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.[Recovery Reserve],
		ODS_DWH_Data.[LOI FEE],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ISNULL(ODS_NetPaid.[Net Paid],0) as 'Net Paid',
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]


		

	into #OCDCCD_Output_Part3

	from 
		#OCDCCD_Output_Part2 ODS_DWH_Data

		left join #ODS_NetPaid ODS_NetPaid
			on ODS_NetPaid.ClaimNumber = ODS_DWH_Data.[ClaimNumber]

	group by 

		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_NetPaid.[Net Paid],
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]


	create index idx1 on #OCDCCD_Output_Part3([ClaimNumber]);
	-- (288000 rows affected) -- 00:11
	-- (288000 rows affected) -- 00:04
	-- (287994 rows affected) -- 00:06


-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 17.4 -- Combine All Temporary Tables Together For Final Query - Part 4/6
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #OCDCCD_Output_Part4

	select 
		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ISNULL(ODS_DWH_Data.[Gross O/S],0)*-1 as 'Gross O/S',
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_DWH_Data.[Net Paid],
		Isnull(ODS_GrossPaid.[Gross Paid],0) as [Gross Paid],
		ISNULL([Gross O/S],0)*-1 + ISNULL([Gross Paid],0) as 'Gross Incurred',
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]


	into #OCDCCD_Output_Part4

	from 
		#OCDCCD_Output_Part3 ODS_DWH_Data

		left join #ODS_GrossPaid ODS_GrossPaid
			on ODS_GrossPaid.ClaimNumber = ODS_DWH_Data.[ClaimNumber]

	group by 

		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_DWH_Data.[Net Paid],
		ODS_GrossPaid.[Gross Paid],
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group]


	create index idx1 on #OCDCCD_Output_Part4([ClaimNumber]);
	-- (288012 rows affected) -- 00:20
	-- (288012 rows affected) -- 00:04
	-- (288012 rows affected) -- 00:06


-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 17.5 -- Combine All Temporary Tables Together For Final Query - Part 5/6
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #OCDCCD_Output_Part5

	select 
		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_DWH_Data.[Net Paid],
		ODS_DWH_Data.[Gross Paid],
		ODS_DWH_Data.[Gross Incurred], 
		Genius_ZKG3_Recovery_Reserve.[Recovery Reserve] as Genius_ZKG3_Recovery_Reserve,
		--rank() over (partition by ODS_DWH_Data.ClaimNumber order by ODS_DWH_Data.ClaimReportDate desc) as 'Rank_s',
		max(Genius_ZKG3_Recovery_Reserve.DateFrom) as Genius_ZKG3_Recovery_Reserve_DateUpdated,
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group],
		Isnull(county_s.County,0) as 'County',
		Case when County is null then 0 Else 1 end as rank_s


	into #OCDCCD_Output_Part5

	from 
		#OCDCCD_Output_Part4 ODS_DWH_Data

		left join #Genius_ZKG3_Recovery_Reserve Genius_ZKG3_Recovery_Reserve
			on Genius_ZKG3_Recovery_Reserve.ClaimNumber = ODS_DWH_Data.[ClaimNumber]
			
		left join #county county_s 
			on county_s.ClaimNumber = ODS_DWH_Data.claimnumber

	group by 

		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_DWH_Data.[Net Paid],
		ODS_DWH_Data.[Gross Paid],
		ODS_DWH_Data.[Gross Incurred],
		Genius_ZKG3_Recovery_Reserve.[Recovery Reserve],
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group],
		county_s.County



	create index idx1 on #OCDCCD_Output_Part5([ClaimNumber]);
	-- (288012 rows affected) -- 00:20
	-- (288012 rows affected) -- 00:05
	-- (288012 rows affected) -- 00:09

-- --------------------------------------------------------------------------------------------------------------------------------------------------
-- Step 17.6 -- Combine All Temporary Tables Together For Final Query - Part 6/6
-- --------------------------------------------------------------------------------------------------------------------------------------------------

	drop table if exists #OCDCCD_Output_Part6

	select 
		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		total_Nett_exrex.Total_Net_OS_EX_REX as 'Total Nett ex REX',
		DWH_DateLoss_Adjuster.[Date Loss Adjuster appointed],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_DWH_Data.[Net Paid],
		ODS_DWH_Data.[Genius_ZKG3_Recovery_Reserve],
		ODS_DWH_Data.[Gross Paid],
		ODS_DWH_Data.[Gross Incurred],
		ODS_DWH_Data.Genius_ZKG3_Recovery_Reserve_DateUpdated,
		ISNULL(ODS_NetOS.NetOS,0)*-1 as 'Net OS',
		ISNULL(ODS_NetOS.NetOS,0)*-1 + ODS_DWH_Data.[Net Paid] as 'Net Incurred',
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group],
		gross_Exp_trans.[Gross Expec Trans] as 'Gross Expected Trans',
		Gross_remaining_trans.[Gross remaining Trans] as 'Gross Remaining Trans',
		Net_Exp_trans.[Net Expec Trans] as 'Net Expected Trans',
		Net_Remaining_Trans.[Net remaining Trans],
		Gross_Recoveries_Recd.[Recovery Received] as 'Gross Recoveries Received',
		Motor_injury_element.[Motor injury Element],[Gross O/S1] as 'Net O/S1',
		ODS_DWH_Data.County as 'County /Risk Address',Rank_s,
		max(rank_s) over (partition by ODS_DWH_Data.ClaimNumber) as 'Real rank'


	into #OCDCCD_Output_Part6

	from 
		#OCDCCD_Output_Part5 ODS_DWH_Data

		left join #ODS_NetOS ODS_NetOS
			on ODS_NetOS.ClaimNumber = ODS_DWH_Data.[ClaimNumber]
		
		left join #total_Nett_exrex total_Nett_exrex on
		total_Nett_exrex.claimnumber = ODS_DWH_Data.claimnumber

		
		left join #DWH_DateLoss_Adjuster DWH_DateLoss_Adjuster on
		DWH_DateLoss_Adjuster.claimnumber = ODS_DWH_Data.claimnumber

		left join #gross_Exp_trans gross_Exp_trans on
		gross_Exp_trans.claimnumber = ODS_DWH_Data.claimnumber

		left join #Net_Exp_trans Net_Exp_trans on
		Net_Exp_trans.claimnumber = ODS_DWH_Data.claimnumber

		left join #Gross_remaining_trans Gross_remaining_trans on
			Gross_remaining_trans.ClaimNumber = ODS_DWH_Data.ClaimNumber

		left join #Net_remaining_trans Net_Remaining_Trans on
			Net_Remaining_Trans.ClaimNumber = ODS_DWH_Data.ClaimNumber

		left join #Gross_Recoveries_Recd Gross_Recoveries_Recd on
		Gross_Recoveries_Recd.ClaimNumber = ODS_DWH_Data.claimnumber

		left join #Motor_injury_element Motor_injury_element on
		Motor_injury_element.ClaimNumber = ODS_DWH_Data.claimnumber

		--where ODS_DWH_Data.Rank_s = 1
		

	group by 

		ODS_DWH_Data.[ClaimKey],
		ODS_DWH_Data.[ClaimNumber],
		ODS_DWH_Data.[Claim Status],
		ODS_DWH_Data.[System],
		ODS_DWH_Data.[ClaimAccidentDate],
		ODS_DWH_Data.[ClaimReportDate],
		ODS_DWH_Data.[ClaimDescription],
		ODS_DWH_Data.[ClaimsettlementDate],
		ODS_DWH_Data.[Resql2],
		ODS_DWH_Data.[Date_Reserve_Last_Updated],
		ODS_DWH_Data.[CustomerPolicyNr],
		ODS_DWH_Data.[brokercode],
		ODS_DWH_Data.[Claimant],
		ODS_DWH_Data.[claimhandlercd],
		ODS_DWH_Data.[Team],
		ODS_DWH_Data.[Cause of Claim],
		ODS_DWH_Data.[Gross O/S],
		ODS_DWH_Data.[Recovery Reserve],		
		ODS_DWH_Data.[LOI FEE],
		total_Nett_exrex.Total_Net_OS_EX_REX ,
		DWH_DateLoss_Adjuster.[Date Loss Adjuster appointed],
		ODS_DWH_Data.[ROI/NI],
		ODS_DWH_Data.ClaimPerilDescription,
		ODS_DWH_Data.[Gross_Recovery_O/S],
		ODS_DWH_Data.[Claim Major Master Code],
		ODS_DWH_Data.[ProductCode],
		ODS_DWH_Data.[Insured],
		ODS_DWH_Data.[FirstName],
		ODS_DWH_Data.[Surname / Company Name],
		ODS_DWH_Data.[Injury Element],
		ODS_DWH_Data.[Claim Last Reopened],
		ODS_DWH_Data.[Claim Reopened Date],
		ODS_DWH_Data.[Most Recent date on KV],
		ODS_DWH_Data.[Or Cur on K9],
		ODS_DWH_Data.[AC Cur on K9],
		ODS_DWH_Data.[Gross O/S1],
		ODS_DWH_Data.[Net Paid],
		ODS_DWH_Data.[Gross Paid],
		ODS_DWH_Data.[Gross Incurred],
		ODS_DWH_Data.[Genius_ZKG3_Recovery_Reserve],
		ODS_DWH_Data.Genius_ZKG3_Recovery_Reserve_DateUpdated,
		ODS_NetOS.NetOS,
		ODS_DWH_Data.[Claim Handler Name],
		ODS_DWH_Data.[Broker Group],
		gross_Exp_trans.[Gross Expec Trans],
		Gross_remaining_trans.[Gross remaining Trans],
		Net_Exp_trans.[Net Expec Trans],
		Net_Remaining_Trans.[Net remaining Trans],
		Gross_Recoveries_Recd.[Recovery Received],
		Motor_injury_element.[Motor injury Element],
		ODS_DWH_Data.County,rank_s




	create index idx1 on #OCDCCD_Output_Part6([ClaimNumber]);
	
	
	-- (288012 rows affected) -- 00:30
	-- (288012 rows affected) -- 00:05
	-- To Rebuild Steps 15 Completely -- 02:19
	-- To Rebuild Steps 15 Completely -- 09:25
	-- To Rebuild Steps 15 Completely -- 16:48

	-- Commit To Physical Table
	/*
	drop table DWH_Sandbox.[dbo].[ODS_OCDCCD_20240731_20240909]
	truncate table DWH_Sandbox.[dbo].[ODS_OCDCCD_20240731_20240909]
	CREATE TABLE DWH_Sandbox.[dbo].[ODS_OCDCCD_20240731_20240909](
	[ClaimKey] [int] NOT NULL,
	[ClaimNumber] [nvarchar](100) NULL,
	[Claim Status] [varchar](6) NOT NULL,
	[System] [char](20) NULL,
	[ClaimAccidentDate] [datetime] NULL,
	[ClaimReportDate] [datetime] NULL,
	[ClaimDescription] [nvarchar](max) NULL,
	[ClaimsettlementDate] [date] NULL,
	[Resql2] [nvarchar](40) NULL,
	[Date_Reserve_Last_Updated] [datetime] NULL,
	[CustomerPolicyNr] [varchar](20) NULL,
	[brokercode] [varchar](10) NULL,
	[Claimant] [nvarchar](50) NULL,
	[claimhandlercd] [char](10) NULL,
	[Team] [char](10) NULL,
	[Cause of Claim] [nvarchar](100) NULL,
	[Gross O/S] [decimal](38, 2) NULL,
	[Net O/S] [float] NULL,
	[Gross Paid] [decimal](38, 2) NULL,
	[Recovery Reserve] [float] NULL,
	[Net Paid] [decimal](38, 2) NULL,
	[ROI/NI] [varchar](3) NOT NULL,
	[ClaimPerilDescription] [nvarchar](100) NULL,
	[Gross_Recovery_O/S] [float] NULL,
	[Claim Major Master Code] [nvarchar](3) NULL,
	[ProductCode] [nvarchar](6) NULL,
	[Insured] [nvarchar](25) NULL,
	[FirstName] [nvarchar](4000) NULL,
	[Surname / Company Name] [nvarchar](25) NULL,
	[Injury Element] [varchar](3) NOT NULL,
	[Claim Last Reopened] [varchar](1) NULL,
	[Claim Reopened Date] [date] NULL,
	[Most Recent date on KV] [date] NULL,
	[Or Cur on K9] [varchar](3) NOT NULL,
	[AC Cur on K9] [varchar](3) NOT NULL,
	[Genius_ZUMA_DateUpdated] [datetime] NULL,
	[Genius_ZNNA_DateUpdated] [datetime] NULL,
	[Genius_ZKFB_DateUpdated] [datetime] NULL,
	[Genius_LKLL_Claim_Reopened_DateUpdated] [datetime] NULL,
	[Genius_LKLL_Recent_KV_Date_DateUpdated] [datetime] NULL,
	[Genius_ZKG0_Date_DateUpdated] [datetime] NULL,
	[Net O/S1] [decimal](38, 2) NULL,
	[Genius_ZKF1_DateUpdated] [datetime] NULL,
	[Net_Paid_ZKG3] [float] NULL,
	[Genius_ZKG3_NetPaid_DateUpdated] [datetime] NULL,
	[Gross_Paid_ZKG3] [float] NULL,
	[Genius_ZKG3_GrossPaid_DateUpdated] [datetime] NULL,
	[Genius_ZKG3_Recovery_Reserve] [float] NULL,
	[Genius_ZKG3_Recovery_Reserve_DateUpdated] [datetime] NULL,
	[ZKG3_Net_OS] [float] NULL,
	[Genius_ZKG3_NetOS_DateUpdated] [datetime] NULL
	) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
	GO
	truncate table DWH_Sandbox.[dbo].[ODS_OCDCCD_20240731_20240904_4]
	
	*/
	-- insert into DWH_Sandbox.[dbo].[ODS_OCDCCD_20240731_20240909] select * from #OCDCCD_Output_Part6

--	Select --[ClaimKey],
--		[ClaimNumber],
--		[CustomerPolicyNr],
--		[Insured],
--		[Claimant],
--		[FirstName],
--		[Surname / Company Name],
--		[System],
--		[Team],
--		[claimhandlercd],
--		[brokercode],
--		[ClaimAccidentDate],
--		[ClaimReportDate],
--		[Date_Reserve_Last_Updated],
--		[Cause of Claim],
--		ClaimPerilDescription,
--		[Resql2],
--		[ClaimDescription],
--		[Gross Incurred],
--		[Gross O/S],
--		[Gross Paid],
--		--[Gross_Recovery_O/S],
--		[Gross O/S1],
--		[Injury Element],
--		[Claim Last Reopened],
--		[Claim Reopened Date],
--		[Most Recent date on KV],
--		[ROI/NI],
--		[Or Cur on K9],
--		[AC Cur on K9],
--		[Claim Status],
--		[ClaimsettlementDate],
--		[Claim Major Master Code],
--		[ProductCode],
--		[Net Paid],
--		[Net OS],
--		[Net Incurred],
--				[Recovery Reserve],	
--		[LOI FEE],
--	[Total Nett ex REX],
--		[Date Loss Adjuster appointed]

-- from #OCDCCD_Output_Part6
---- Where ClaimNumber in ('')
-- group by
-- [ClaimNumber],
--		[CustomerPolicyNr],
--		[Insured],
--		[Claimant],
--		[FirstName],
--		[Surname / Company Name],
--		[System],
--		[Team],
--		[claimhandlercd],
--		[brokercode],
--		[ClaimAccidentDate],
--		[ClaimReportDate],
--		[Date_Reserve_Last_Updated],
--		[Cause of Claim],
--		ClaimPerilDescription,
--		[Resql2],
--		[ClaimDescription],
--		[Gross Incurred],
--		[Gross O/S],
--		[Gross Paid],
--		--[Gross_Recovery_O/S],
--		[Gross O/S1],
----[Recovery Reserve],		
--	--LOI FEE],
--		[Injury Element],
--		[Claim Last Reopened],
--		[Claim Reopened Date],
--		[Most Recent date on KV],
--		[ROI/NI],
--		[Or Cur on K9],
--		[AC Cur on K9],
--		[Claim Status],
--		[ClaimsettlementDate],
--		[Claim Major Master Code],
--		[ProductCode],
--		[Net Paid],
--		[Net OS],
--		[Net Incurred],
--		[Recovery Reserve],	
--		[LOI FEE],
--		[Total Nett ex REX],
--		[Date Loss Adjuster appointed]

 
Select
					[ClaimNumber],
				[CustomerPolicyNr],
				[Insured],
				[Claimant],
				[FirstName],
				[Surname / Company Name],
				[System],
				[Team],
				[claimhandlercd],
				[brokercode],
				[ClaimAccidentDate],
				[ClaimReportDate],
				[Date_Reserve_Last_Updated],
				[Cause of Claim],
				[ClaimDescription],
				[Resql2],
				ClaimPerilDescription,
				[Gross Incurred],
				[Gross O/S],
				[Gross Paid],
				[Gross O/S1] as [Total Gross O/S Ex REX],
				[Recovery Reserve],
				[LOI FEE],
				[Injury Element],
				[Claim Last Reopened],
				[Claim Reopened Date],
				[Most Recent date on KV],
				[ROI/NI],
				[Or Cur on K9],
				[AC Cur on K9],
				[Claim Status],
				[Claim Major Master Code],
				[ProductCode],
				[Net Paid],
				[Net OS],
				[Net Incurred],
				[Total Nett ex REX],
				[Date Loss Adjuster appointed],
				[ClaimsettlementDate],
				[Claim Handler Name],
				[Broker Group],
				[Gross Expected Trans],
				[Gross Remaining Trans],
				[Net Expected Trans],
				[Net remaining Trans],
				[Gross Recoveries Received],
				[Motor injury Element],[Net O/S1],
				[County /Risk Address]


from #OCDCCD_Output_Part6
--where ClaimNumber in ('')
where  [Real rank] = Rank_s
	group by
						[ClaimNumber],
				[CustomerPolicyNr],
				[Insured],
				[Claimant],
				[FirstName],
				[Surname / Company Name],
				[System],
				[Team],
				[claimhandlercd],
				[brokercode],
				[ClaimAccidentDate],
				[ClaimReportDate],
				[Date_Reserve_Last_Updated],
				[Cause of Claim],
				[ClaimDescription],
				[Resql2],
				ClaimPerilDescription,
				[Gross Incurred],
				[Gross O/S],
				[Gross Paid],
				[Gross O/S1],
				[Recovery Reserve],
				[LOI FEE],
				[Injury Element],
				[Claim Last Reopened],
				[Claim Reopened Date],
				[Most Recent date on KV],
				[ROI/NI],
				[Or Cur on K9],
				[AC Cur on K9],
				[Claim Status],
				[Claim Major Master Code],
				[ProductCode],
				[Net Paid],
				[Net OS],
				[Net Incurred],
				[Total Nett ex REX],
				[Date Loss Adjuster appointed],
				[ClaimsettlementDate],
				[Claim Handler Name],
				[Broker Group],
				[Gross Expected Trans],
				[Gross Remaining Trans],
				[Net Expected Trans],
				[Net remaining Trans],
				[Gross Recoveries Received],
				[Motor injury Element],[Net O/S1],
				[County /Risk Address]


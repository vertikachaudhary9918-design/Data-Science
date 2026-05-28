USE [ODS]
GO

/****** Object:  StoredProcedure [ODS].[sp_populate_DWH_3_tran_view_sales_and_growth_temp_inserts]    Script Date: 27/05/2026 11:31:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO









CREATE PROCEDURE [ODS].[sp_populate_DWH_3_tran_view_sales_and_growth_temp_inserts]
AS
BEGIN

exec [ODS].[sp_etl_audit_insert_row] '[ODS].[sp_populate_dwh_3_tran_view_sales_and_growth_temp_inserts]','** BEGIN **]' 
	
drop table if exists [DWH_TMP].[FACT].CountDates;

select a.PolicySk, Max(a.TransCtDt) as 'TransCtDt'
into [DWH_TMP].[FACT].CountDates
from
	(select b.PolicySK,
		case when b.PolicyStartDtSK > b.PolicyWrittenDtSK then b.PolicyStartDt else b.PolicyWrittenDt end as 'TransCtDt'
	from ods.Policy b
	where
		b.CurrentRow = 1
	) a
group by a.PolicySk


-- Mid-Term Cancellation (MTC) and Not Taken Up (NTU) policies are counted as -1 on the last transaction date of the policy.
--  [DWH_TMP].[FACT].MaxTran selects these dates for each policy
 drop table if exists [DWH_TMP].[FACT].MaxTran;
 
select po.policysk, max(dt2.FullDate) as 'MTDate'
into [DWH_TMP].[FACT].MaxTran
from ods.Policy po
left join ods.policytransaction pt2
	on po.PolicySK = pt2.PolicySK
left join ods.Date dt2
	on pt2.PolicyTransactionDTSK = dt2.DateSK
where
	po.CurrentRow = 1
and
	pt2.CurrentRow = 1
and
	dt2.CurrentRow = 1
group by po.PolicySK;



-- Personal Line Converted Quote Count is based on New Business Policy SOld date
drop table if exists [DWH_TMP].[FACT].MinTran;

select po.policysk, min(dt2.DateSK) -1 as 'MinDateSK' -- EDI Transaction Entry Date is one day greater than policy sold date
into [DWH_TMP].[FACT].MinTran
from ods.Policy po
left join ods.policytransaction pt2
	on po.PolicySK = pt2.PolicySK
left join ods.Date dt2
	on  [PolicyTransactionEntryDT]  = dt2.FullDate --datediff(day, -1, pt2.[PolicyTransactionEntryDT])
where
	po.PolicyMasterSeq in ('0','1') -- 'New Business' Transaction Type
and  
	po.PolicyStatusSK in ('1','2','3','4') --New Business, Renewal, MTC, Lapsed
and
	po.PolicyStartDtSK != po.PolicyExpiryDtSK -- Remove NTU marked as MTC
and	
	po.CurrentRow = 1
and
	pt2.CurrentRow = 1
and
	dt2.CurrentRow = 1
group by po.PolicySK;


-- The coverge table contains duplicate Coverages, as each time a policy is updated, a new coverage record is inserted.
-- To prevent double-counting of transactions, A distinct list of Coverages, as well as the transaction type / code, and re-insurance indicator 
-- required for metric calculations are selected. Transactions amounts are then calculated for this list for use throughout the script
drop table if exists [DWH_TMP].[FACT].CoverageAmounts_standard

select a.*
		, b.policysk
		, c.datesk 
		, sum(PolicyTransactionAmtRpt)	 as 'PolicyTransactionAmtRpt'
		, sum(PolicyTransactionAmtLcl)	 as 'PolicyTransactionAmtLcl'
		, sum(PolicyTransactionCommRpt)	 as 'PolicyTransactionCommRpt'
		, sum(PolicyTransactionCommLcl)	 as 'PolicyTransactionCommLcl'
into [DWH_TMP].[FACT].CoverageAmounts_standard
from
(
	select cov.[ClassOfBusinessSK]
			, cov.CoverageSK
			, coalesce(br.brokersk,p.brokersk) as brokersk -- added amit
			, a.PolicyTransactionTypeSK
			, b.TransactionTypeCode
			, a.PolicyTransactionReInsuranceInd
	from ods.Coverage cov
	join ods.policytransaction a --added amit
		on cov.CoverageSk = a.CoverageSk
	join ods.policy p on a.policysk = p.policysk
	left join (select * from ods.broker where currentrow = 1) br on br.BrokerCode = a.TransactionBrokerCode -- added amit
	join ods.TransactionType b
		on a.PolicyTransactionTypeSK = b.TransactionTypeSK
	where
		cov.CurrentRow = 1		
	and
		a.CurrentRow = 1
	and
		b.CurrentRow = 1
	and p.CurrentRow = 1
	group by  cov.[ClassOfBusinessSK]
			, cov.CoverageSK
			, coalesce(br.brokersk,p.brokersk) -- added amit
			, a.PolicyTransactionReInsuranceInd
			, a.PolicyTransactionTypeSK
			, b.TransactionTypeCode
) a
join 
(
	select coalesce(br.brokersk,p.brokersk) as brokersk,a.PolicyTransactionReInsuranceInd,a.PolicySK,PolicyTransactionTypeSK,PolicyTransactionDT,PolicyClaimsExcludeInd,a.CurrentRow,a.CoverageSk,PolicyTransactionAmtRpt,PolicyTransactionAmtLcl,PolicyTransactionCommRpt,PolicyTransactionCommLcl
	from ods.policytransaction  a --added amit
	join ods.policy p on a.policysk = p.policysk
	left join (select * from ods.broker where currentrow = 1) br on br.BrokerCode = a.TransactionBrokerCode --added amit
	where a.currentrow = 1 -- added amit
	and p.CurrentRow = 1
)b on a.CoverageSk = b.CoverageSk and a.PolicyTransactionReInsuranceInd = b.PolicyTransactionReInsuranceInd and a.PolicyTransactionTypeSK = b.PolicyTransactionTypeSK and b.brokersk = a.brokersk --added amit
join ods.[Date] c on EOMonth(b.PolicyTransactionDT) = c.FullDate
where
b.PolicyClaimsExcludeInd = 0
and b.CurrentRow = 1
and c.CurrentRow = 1
group by	 
	b.policysk
	, a.[ClassOfBusinessSK]
	, a.CoverageSK
	, c.DateSk
	, a.brokersk -- added amit
	, a.PolicyTransactionReInsuranceInd
	, a.PolicyTransactionTypeSK
	, a.TransactionTypeCode;


--///////////////////////////////////////// GWP////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionGrossWrittenPremium; --added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.[PolicyTransactionAmtRpt]) as 'GrossWrittenPremiumRptAmt'			-- Euro Amount
		,sum(cd.[PolicyTransactionAmtLcl]) as 'GrossWrittenPremiumLclAmt'			-- Local amount. Sterling in case of NI policies
into  [DWH_TMP].[FACT].transactionGrossWrittenPremium --added amit
from ods.[Policy] b
join [DWH_TMP].[FACT].CoverageAmounts_standard cd ON b.PolicySK = cd.PolicySK --Added PolicySk condition for GWP-----
join ods.[Date] c on  cd.datesk = c.DateSK
where  
	b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 
	and cd.PolicyTransactionReInsuranceInd = 0		-- Policy- Level Re-insurance Included
	and b.CurrentRow = 1
	and c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey;


--///////////////////////////////////////// Base Commission////////////////////////////////////
drop table if exists  [DWH_TMP].[FACT].transactionBaseCommission; -- added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		, -sum(cd.PolicyTransactionCommRpt) as 'BaseCommissionRptAmt'			-- Euro Amount
		, -sum(cd.PolicyTransactionCommLcl) as 'BaseCommissionLclAmt'			-- Local amount. Sterling in case of NI policies
into [DWH_TMP].[FACT].transactionBaseCommission -- added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where  b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	cd.PolicyTransactionReInsuranceInd = 0		-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) -- added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey;

--///////////////////////////////////////// Renewal GWP////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionRenewalGWP; --added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.PolicyTransactionAmtRpt) as 'RNGWPRptAmt'
		,sum(cd.[PolicyTransactionAmtLcl]) as 'RNGWPLclAmt'
into [DWH_TMP].[FACT].transactionRenewalGWP --added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where  b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	cd.TransactionTypeCode = 'REN' -- 'Renewal' Transaction Type
and 
	cd.PolicyTransactionReInsuranceInd = 0		-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey

	
--///////////////////////////////////////// NWP////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionNetWrittenPremium; --added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.[PolicyTransactionAmtRpt]) as 'NetWrittenPremiumRptAmt'
		,sum(cd.[PolicyTransactionAmtLcl]) as 'NetWrittenPremiumLclAmt'
into [DWH_TMP].[FACT].transactionNetWrittenPremium --added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) --added amit
		,b.PolicyUnderwriterSK -- amit
		,b.PolicyCurrencyKey


--///////////////////////////////////////// NB GWP////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionNBGWP; -- added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.PolicyTransactionAmtRpt) as 'NBGWPRptAmt'
		,sum(cd.[PolicyTransactionAmtLcl]) as 'NBGWPLclAmt'
into [DWH_TMP].[FACT].transactionNBGWP -- added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where  b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	cd.TransactionTypeCode in ('NEW','NTU') -- 'New Business' and 'NTU' Transaction Type
and 
	cd.PolicyTransactionReInsuranceInd = 0		-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey


--///////////////////////////////////////// NB MTA Amount////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionNBMidTermAdj; --added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.[PolicyTransactionAmtLcl]) as 'NBMidTermAdjLclAmt'
		,sum(cd.[PolicyTransactionAmtRpt]) as 'NBMidTermAdjRptAmt'
into [DWH_TMP].[FACT].transactionNBMidTermAdj --added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where  b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	b.PolicyMasterSeq in ('0','1')				-- 'New Business' 
and 
	cd.TransactionTypeCode in ('MTA','AGY')		-- 'MTA', 'Agency Transfer' Transaction type
and 
	cd.PolicyTransactionReInsuranceInd =0		-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) -- added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey;


--///////////////////////////////////////// NB MTC Amount////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionNBMidTermCancel; -- added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.[PolicyTransactionAmtRpt]) as 'NBMidTermCancRptAmt'
		,sum(cd.[PolicyTransactionAmtLcl]) as 'NBMidTermCancLclAmt'
into [DWH_TMP].[FACT].transactionNBMidTermCancel -- added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where   b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	b.PolicyMasterSeq in ('0','1')			-- 'New Business' 
and 
	cd.TransactionTypeCode  = 'MTC'			-- 'MTC' Transaction type
and 
	cd.PolicyTransactionReInsuranceInd = 0	-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) -- added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey;


--///////////////////////////////////////// Renewal MTA Amount////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionRenewalMidTermAdj; -- added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.[PolicyTransactionAmtRpt]) as 'RNMidTermAdjRptAmt'
		,sum(cd.[PolicyTransactionAmtLcl]) as 'RNMidTermAdjLclAmt'
into [DWH_TMP].[FACT].transactionRenewalMidTermAdj -- added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	b.PolicyMasterSeq  > '1'				-- 'Renewal' 
and 
	cd.TransactionTypeCode in ('MTA','AGY')		-- 'MTA' Transaction type
and 
	cd.PolicyTransactionReInsuranceInd = 0		-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) -- added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey;


--///////////////////////////////////////// Renewal MTC Amount////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionRenewalMidTermCancel; --added amit

select cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) as brokersk --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey
		,sum(cd.[PolicyTransactionAmtRpt]) as 'RNMidTermCancRptAmt'
		,sum(cd.[PolicyTransactionAmtLcl]) as 'RNMidTermCancLclAmt'
into [DWH_TMP].[FACT].transactionRenewalMidTermCancel --added amit
from ods.[Policy] b
join ods.PolicyCoverageBridge pcb
	on b.PolicySK = pcb.PolicySK
join [DWH_TMP].[FACT].CoverageAmounts_standard cd
	on pcb.Coveragesk = cd.Coveragesk
join ods.[Date] c
	on  cd.datesk = c.datesk
where  b.PolicyMasterSeq <> 999 and b.PolicyMasterSeq > 0 --amit
and 
	b.PolicyMasterSeq  > '1'				-- 'Renewal' 
and 
	cd.TransactionTypeCode  = 'MTC'		-- 'MTC' Transaction type
and 
	cd.PolicyTransactionReInsuranceInd = 0		-- Policy- Level Re-insurance Included
and
		b.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		c.CurrentRow = 1
group by cd.DateSK
		,cd.[ClassOfBusinessSK]
		,b.ProductSK
		,b.SubProductSK
		,b.ChannelSK
		,coalesce(cd.BrokerSK,b.brokersk) --added amit
		,b.PolicyUnderwriterSK
		,b.PolicyCurrencyKey;
		

--///////////////////////////////////////// Live Policy COunt////////////////////////////////////
drop table if exists [DWH_TMP].FACT.transactionLivePolicy;

select b.DateSK
			,cov.[ClassOfBusinessSK]
			,a.ProductSK
			,a.SubProductSK
			,a.ChannelSK
			,a.BrokerSK
			,coalesce(a.PolicyUnderwriterSK,0) as PolicyUnderwriterSK
			,a.PolicyCurrencyKey
			,count(distinct a.PolicySK) as 'LivePolicyCnt'-- COunt of policies
	into [DWH_TMP].FACT.transactionLivePolicy
	from [ODS].[Date] b 
	left join [ODS].[Policy] a
		on a.PolicyStartDtsk <= b.DateSK		-- Policy was active at end of month
		and a.PolicyExpiryDtSK > b.DateSK		-- Policy is still active following end of month
	join [ODS].[PolicyCoverageBridge] pcb		--Link to coverage bridge
		on a.PolicySK = pcb.policysk
	join [ODS].[Coverage] cov					--LInk to COverage for CoB
		on pcb.CoverageSk = cov.CoverageSk 
	where  b.FullDate = EOMONTH(b.FullDate)		-- only take dates at end of month
		and b.CalendarYear >=2016				-- Calculation limited to dates in Dashboard
and a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- added by Monica Dhamale on 7/12/2018 for Release 6.0 (AREV Migration Fix)			
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		b.CurrentRow = 1
and 
		a.productsk not in (78,109) -- PRODUCTSKs OF RINONP + RIPPN - Both Reinsurance Products - KM 07/05/2019																								 
group by b.DateSK
			,cov.[ClassOfBusinessSK]
			,a.ProductSK
			,a.SubProductSK
			,a.ChannelSK
			,a.BrokerSK
			,coalesce(a.PolicyUnderwriterSK,0)
			,a.PolicyCurrencyKey
			

--///////////////////////////////////////// Written Policy COunt////////////////////////////////////
-- Written Policies are calculated using the following steps:
-- 1. Count Policies with Written/Inception dates for a given Month / Broker / Product / etc.
-- 2. Count Policies which NTU'd for a given Month / Broker / Product / etc.
-- 3. Create table of MTC'd  for a given Month / Broker / Product / etc.
-- 4.	a) Create a table for all DWH records with a policy written or NTU'd
--		b) Subtract NTUs from Written polices to give Written Policy Count
-- 4.	a) Create a table for all DWH records with a policy written, NTU'd, or MTC'd
--		b) Subtract NTUs and MTCs from Written polices to give Written Policy  Count excl. MTC

drop table if exists [DWH_TMP].[FACT].transactionWrittenPolicy_Policies; --added amit

select  
		D.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'PolicyCount'
into [DWH_TMP].[FACT].transactionWrittenPolicy_Policies --added amit
from ods.[Policy] a
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk 
join [DWH_TMP].[FACT].CountDates c
	on a.PolicySK = c.PolicySK
join ods.[Date] d
	on eomonth(c.TransCtDt) = d.FullDate
where a.PolicyStatusSK in ('1','2','3','4', '5')	-- New Business, Renewal, MTC, Lapsed, NTU. NTU Will be subracted later
and a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- added by Monica Dhamale on 7/12/2018 for Release 6.0 (AREV Migration Fix)
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by 
		d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;
		

drop table if exists [DWH_TMP].[FACT].transactionWrittenPolicy_NTU; -- added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'NTUCount'
into [DWH_TMP].[FACT].transactionWrittenPolicy_NTU -- added amit
from  ods.[Policy] a
join [DWH_TMP].[FACT].MaxTran mt
	ON mt.PolicySK = a.PolicySK
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk
join ods.[Date] d
	on eomonth(mt.MTDate) = d.FullDate
where a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- amit
and 
	(
	a.PolicyStatusSk = '5'
	OR (a.PolicyStatusSk = '3'
		and a.PolicyStartDtSK = a.PolicyExpiryDtSK)	
	) --amit
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;


drop table if exists [DWH_TMP].[FACT].transactionWrittenPolicy_MTC; -- added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'MTCCount'
into [DWH_TMP].[FACT].transactionWrittenPolicy_MTC -- added amit
from  ods.[Policy] a
join [DWH_TMP].[FACT].MaxTran mt
	ON mt.PolicySK = a.PolicySK
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk
join ods.[Date] d
	on eomonth(mt.MTDate) = d.FullDate
where a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- amit
and 
	a.PolicyStatusSK = '3'
and 
	a.PolicyStartDtSK != a.PolicyExpiryDtSK	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;
		

drop table if exists [DWH_TMP].[FACT].transactionWrittenPolicy; --added amit

select a.DateSK
		,a.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,ISNULL(b.PolicyCount, 0 ) - ISNULL(c.NTUCount, 0 ) as 'WrittenPolicyCnt'
into [DWH_TMP].[FACT].transactionWrittenPolicy --added amit
from
(
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionWrittenPolicy_Policies --added amit
	union
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionWrittenPolicy_NTU
) a
left join [DWH_TMP].[FACT].transactionWrittenPolicy_Policies b --added amit
	on a.DateSK							= b.DateSK
		and a.[ClassOfBusinessSK]		= b.[ClassOfBusinessSK]
		and a.ProductSK					= b.ProductSK
		and a.SubProductSK				= b.SubProductSK
		and a.ChannelSK					= b.ChannelSK
		and a.BrokerSK					= b.BrokerSK
		and a.PolicyUnderwriterSK		= b.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= b.PolicyCurrencyKey
left join [DWH_TMP].[FACT].transactionWrittenPolicy_NTU c --added amit
	on a.DateSK							= c.DateSK
		and a.[ClassOfBusinessSK]		= c.[ClassOfBusinessSK]
		and a.ProductSK					= c.ProductSK
		and a.SubProductSK				= c.SubProductSK
		and a.ChannelSK					= c.ChannelSK
		and a.BrokerSK					= c.BrokerSK
		and a.PolicyUnderwriterSK		= c.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= c.PolicyCurrencyKey;


drop table if exists [DWH_TMP].[FACT].transactionWrittenPolicyExclMTCCnt; -- added amit

select a.DateSK
		,a.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,ISNULL(b.PolicyCount, 0 ) - ISNULL(c.NTUCount, 0 ) - ISNULL(d.MTCCount, 0) as 'WrittenPolicyExclMTCCnt'
into [DWH_TMP].[FACT].transactionWrittenPolicyExclMTCCnt --added amit
from
(
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionWrittenPolicy_Policies --added amit
	union
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionWrittenPolicy_NTU --added amit
	union
	select DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionWrittenPolicy_MTC --added amit

) a
left join [DWH_TMP].[FACT].transactionWrittenPolicy_Policies b --added amit
	on a.DateSK							= b.DateSK
		and a.[ClassOfBusinessSK]		= b.[ClassOfBusinessSK]
		and a.ProductSK					= b.ProductSK
		and a.SubProductSK				= b.SubProductSK
		and a.ChannelSK					= b.ChannelSK
		and a.BrokerSK					= b.BrokerSK
		and a.PolicyUnderwriterSK		= b.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= b.PolicyCurrencyKey
left join [DWH_TMP].[FACT].transactionWrittenPolicy_NTU c --added amit
	on a.DateSK							= c.DateSK
		and a.[ClassOfBusinessSK]		= c.[ClassOfBusinessSK]
		and a.ProductSK					= c.ProductSK
		and a.SubProductSK				= c.SubProductSK
		and a.ChannelSK					= c.ChannelSK
		and a.BrokerSK					= c.BrokerSK
		and a.PolicyUnderwriterSK		= c.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= c.PolicyCurrencyKey
left join [DWH_TMP].[FACT].transactionWrittenPolicy_MTC d --added amit
	on a.DateSK							= d.DateSK
		and a.[ClassOfBusinessSK]		= d.[ClassOfBusinessSK]
		and a.ProductSK					= d.ProductSK
		and a.SubProductSK				= d.SubProductSK
		and a.ChannelSK					= d.ChannelSK
		and a.BrokerSK					= d.BrokerSK
		and a.PolicyUnderwriterSK		= d.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= d.PolicyCurrencyKey;


--///////////////////////////////////////// NB Written Policy COunt////////////////////////////////////
--///////////////////////////////////////// NB NTU COunt////////////////////////////////////
-- NB Written Policies are calculated using the following steps:
-- 1. Count NB Policies with Written/Inception dates for a given Month / Broker / Product / etc.
-- 2. Count NB Policies which NTU'd for a given Month / Broker / Product / etc.
-- 4.	a) Create a table for all DWH records with a policy written or NTU'd
--		b) Subtract NTUs from Written polices to give Written Policy Count
-- 3. Create table of NB Policies MTC'd  for a given Month / Broker / Product / etc.


drop table if exists [DWH_TMP].[FACT].transactionNBWrittenPolicy_Policies; --added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'PolicyCount'
into [DWH_TMP].[FACT].transactionNBWrittenPolicy_Policies --added amit
from ods.[Policy] a
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk 
join [DWH_TMP].[FACT].CountDates c
	on a.PolicySK = c.PolicySK
join ods.[Date] d
	on eomonth(c.TransCtDt) = d.FullDate
where  a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- amit
and 
	a.PolicyStatusSK in ('1','2','3','4', '5')	-- New Business, Renewal, MTC, Lapsed, NTU. NTU Will be subracted later
and 
	a.PolicyMasterSeq in ('0','1')	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;


drop table if exists [DWH_TMP].[FACT].transactionNBNotTakenU; -- added amit

select * from ods.Status where StatusSource = 'Genius'

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'NBNotTakenUpCnt'
into [DWH_TMP].[FACT].transactionNBNotTakenU -- added amit
from ods.Policy a
join [DWH_TMP].[FACT].MaxTran mt
	ON mt.PolicySK = a.PolicySK
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk
join ods.[Date] d
	on eomonth(mt.MTDate) = d.FullDate
where a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- amit
and
	a.PolicyMasterSeq in ('0','1')
and
	(
		a.PolicyStatusSk = '5'
		OR (a.PolicyStatusSk = '3'
			and a.PolicyStartDtSK = a.PolicyExpiryDtSK)
	)	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;


drop table if exists [DWH_TMP].[FACT].transactionNBWrittenPolicy; -- added amit
select a.DateSK
		,a.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,ISNULL(b.PolicyCount, 0 ) - ISNULL(c.NBNotTakenUpCnt, 0 ) as 'NBWrittenPolicyCnt'
into [DWH_TMP].[FACT].transactionNBWrittenPolicy -- added amit
from
(
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionNBWrittenPolicy_Policies -- added amit
	union
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionNBNotTakenU -- added amit
) a
left join [DWH_TMP].[FACT].transactionNBWrittenPolicy_Policies b -- added amit
	on a.DateSK							= b.DateSK
		and a.[ClassOfBusinessSK]		= b.[ClassOfBusinessSK]
		and a.ProductSK					= b.ProductSK
		and a.SubProductSK				= b.SubProductSK
		and a.ChannelSK					= b.ChannelSK
		and a.BrokerSK					= b.BrokerSK
		and a.PolicyUnderwriterSK		= b.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= b.PolicyCurrencyKey
left join [DWH_TMP].[FACT].transactionNBNotTakenU c -- added amit
	on a.DateSK							= c.DateSK
		and a.[ClassOfBusinessSK]		= c.[ClassOfBusinessSK]
		and a.ProductSK					= c.ProductSK
		and a.SubProductSK				= c.SubProductSK
		and a.ChannelSK					= c.ChannelSK
		and a.BrokerSK					= c.BrokerSK
		and a.PolicyUnderwriterSK		= c.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= c.PolicyCurrencyKey;
		

--///////////////////////////////////////// NB MTC COunt////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionNBMidTermCancelCnt; -- added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'NBMidTermCancCount'
into [DWH_TMP].[FACT].transactionNBMidTermCancelCnt -- added amit
from  ods.[Policy] a
join [DWH_TMP].[FACT].MaxTran mt
	ON mt.PolicySK = a.PolicySK
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk
join ods.[Date] d
	on eomonth(mt.MTDate) = d.FullDate
where a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- amit
and 
	PolicyMasterSeq in ('0','1')
and	
	a.PolicyStatusSK = '3'
and 
	a.PolicyStartDtSK != a.PolicyExpiryDtSK	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;
		

--///////////////////////////////////////// RN Premium Offered ////////////////////////////////////
--Not Applicable in Transaction view. These metrics are based on Expiry date, and so are not calculated in the Transaction View

--///////////////////////////////////////// RN Premium Achieved ////////////////////////////////////
--Not Applicable in Transaction view. These metrics are based on Expiry date, and so are not calculated in the Transaction View

--///////////////////////////////////////// RN Premium Achieved////////////////////////////////////
--Not Applicable in Transaction view. These metrics are based on Expiry date, and so are not calculated in the Transaction View

--///////////////////////////////////////// RN Offered COunt////////////////////////////////////
--Not Applicable in Transaction view. These metrics are based on Expiry date, and so are not calculated in the Transaction View

--///////////////////////////////////////// RN Achieved COunt////////////////////////////////////
--Not Applicable in Transaction view. These metrics are based on Expiry date, and so are not calculated in the Transaction View

--///////////////////////////////////////// RN Lapsed COunt////////////////////////////////////
--Not Applicable in Transaction view. These metrics are based on Expiry date, and so are not calculated in the Transaction View

--///////////////////////////////////////// RN Written Policy COunt////////////////////////////////////
--///////////////////////////////////////// RN NTU COunt////////////////////////////////////
-- NB Written Policies are calculated using the following steps:
-- 1. Count RN Policies with Written/Inception dates for a given Month / Broker / Product / etc.
-- 2. Count RN Policies which NTU'd for a given Month / Broker / Product / etc.
-- 4.	a) Create a table for all DWH records with a policy written or NTU'd
--		b) Subtract NTUs from Written polices to give Written Policy Count
-- 3. Create table of RN Policies MTC'd  for a given Month / Broker / Product / etc.

drop table if exists [DWH_TMP].[FACT].transactionRNWrittenPolicy_Policies; -- added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'RenewalWrittenPolicyCnt'
into [DWH_TMP].[FACT].transactionRNWrittenPolicy_Policies -- added amit
from ods.[Policy] a
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk 
join [DWH_TMP].[FACT].CountDates c
	on a.PolicySK = c.PolicySK
join ods.[Date] d
	on eomonth(c.TransCtDt) = d.FullDate
where a.PolicyStatusSK in ('1','2','3','4', '5')	-- New Business, Renewal, MTC, Lapsed, NTU. NTU Will be subracted later
and 
	a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- added by Monica Dhamale on 7/12/2018 for Release 6.0 (AREV Migration Fix)
and 
	a.PolicyMasterSeq > 1	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;
		

drop table if exists [DWH_TMP].[FACT].transactionRNNotTakenU; -- added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'RNNTUCount'
into [DWH_TMP].[FACT].transactionRNNotTakenU -- added amit
from ods.Policy a
join [DWH_TMP].[FACT].MaxTran mt
	ON mt.PolicySK = a.PolicySK
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk
join ods.[Date] d
	on eomonth(mt.MTDate) = d.FullDate
where a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- added by Monica Dhamale on 7/12/2018 for Release 6.0 (AREV Migration Fix)		
and   
	a.PolicyMasterSeq > 1
and
	(
		a.PolicyStatusSk = '5'
		OR (a.PolicyStatusSk = '3'
			and a.PolicyStartDtSK = a.PolicyExpiryDtSK)
	)	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;


drop table if exists [DWH_TMP].[FACT].transactionRNWrittenPolicy; -- added amit

select a.DateSK
		,a.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,ISNULL(b.RenewalWrittenPolicyCnt, 0 ) - ISNULL(c.RNNTUCount, 0 ) as 'RNWrittenPolicyCnt'
into [DWH_TMP].[FACT].transactionRNWrittenPolicy -- added amit
from
(
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionRNWrittenPolicy_Policies -- added amit
	union
	select  DateSK
			,[ClassOfBusinessSK]
			,ProductSK
			,SubProductSK
			,ChannelSK
			,BrokerSK
			,PolicyUnderwriterSK
			,PolicyCurrencyKey
	from [DWH_TMP].[FACT].transactionRNNotTakenU -- added amit
) a
left join [DWH_TMP].[FACT].transactionRNWrittenPolicy_Policies b -- added amit
	on a.DateSK							= b.DateSK
		and a.[ClassOfBusinessSK]		= b.[ClassOfBusinessSK]
		and a.ProductSK					= b.ProductSK
		and a.SubProductSK				= b.SubProductSK
		and a.ChannelSK					= b.ChannelSK
		and a.BrokerSK					= b.BrokerSK
		and a.PolicyUnderwriterSK		= b.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= b.PolicyCurrencyKey
left join [DWH_TMP].[FACT].transactionRNNotTakenU c -- added amit
	on a.DateSK							= c.DateSK
		and a.[ClassOfBusinessSK]		= c.[ClassOfBusinessSK]
		and a.ProductSK					= c.ProductSK
		and a.SubProductSK				= c.SubProductSK
		and a.ChannelSK					= c.ChannelSK
		and a.BrokerSK					= c.BrokerSK
		and a.PolicyUnderwriterSK		= c.PolicyUnderwriterSK
		and a.PolicyCurrencyKey			= c.PolicyCurrencyKey;


--///////////////////////////////////////// RN MTC COunt////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].transactionRNMidTermCancelCnt; -- added amit

select  d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey
		,count(distinct a.PolicySK) as 'RNMidTermCancCount'
into [DWH_TMP].[FACT].transactionRNMidTermCancelCnt -- added amit
from ods.Policy a
join [DWH_TMP].[FACT].MaxTran mt
	ON mt.PolicySK = a.PolicySK
join ods.PolicyCoverageBridge pcb
	on a.PolicySK = pcb.PolicySK
join  ods.[Coverage] cov
	on pcb.CoverageSk = cov.CoverageSk
join ods.[Date] d
	on eomonth(mt.MTDate) = d.FullDate
where a.PolicyMasterSeq <> 999 and a.PolicyMasterSeq > 0 -- added by Monica Dhamale on 7/12/2018 for Release 6.0 (AREV Migration Fix)
and PolicyMasterSeq > 1
	and	a.PolicyStatusSK = '3'
	and a.PolicyStartDtSK != a.PolicyExpiryDtSK	
and
		a.CurrentRow = 1
and
		pcb.CurrentRow = 1
and
		cov.CurrentRow = 1
and
		d.CurrentRow = 1
group by d.DateSK
		,cov.[ClassOfBusinessSK]
		,a.ProductSK
		,a.SubProductSK
		,a.ChannelSK
		,a.BrokerSK
		,a.PolicyUnderwriterSK
		,a.PolicyCurrencyKey;


--/////////////////////////////////////////Personal Lines Quote Count////////////////////////////////////
-- MARK 8/7/2019 - UPDATED COLUMNS TO SUIT TRAN 

drop table if exists [DWH_TMP].[FACT].[transactionPLQuoteCnt];  -- added amit

select 
	e.DateSK
	,a.[ClassOfBusinessSK]
	,b.ProductSK AS [ProductSK]
	,'0' AS [SubProductSK]
	,d.ChannelSK
	,a.BrokerSK 
	,a.UnderwriterSK as PolicyUnderwriterSK
	,a.QuoteCurrencySK  as PolicyCurrencyKey
	,count(a.QuoteSk) as 'PLQuoteCnt' 
into 
	[DWH_TMP].[FACT].[transactionPLQuoteCnt]  -- added amit
from 
	ods.[Quote] a
join 
	--select * from 
	ods.QuotePremium b
on
	a.QuoteSk = b.QuoteSk
-- join
-- 	ods.[Policy] pol
-- on
-- 	a.PolicySK = pol.PolicySK
join 
	ods.[Date] e 
on 
	-- eomonth(pol.[PolicyStartDt]) = e.FullDate
	eomonth(a.[DateCreate]) = e.FullDate
join
	ods.Channel d
on 
	a.QuoteChannelSk = d.ChannelSk
join
	ods.ClassOfBusiness cob
on
	a.ClassOfBusinessSK = cob.ClassOfBusinessSK
where
	a.[CountableQuoteTransInd] = 1
-- and
	-- b.OnCoverInd = 1
and
	cob.ClassOfBusinessName in ('Direct Household','Direct Motor','Intermediated Household','Intermediated Motor')
	and a.CurrentRow = 1
	and	b.CurrentRow =1
	-- and pol.CurrentRow = 1
	and e.CurrentRow = 1
	and d.CurrentRow = 1
	and cob.CurrentRow = 1
	and a.[CountableQuoteTransInd] = 1
	-- and	b.OnCoverInd = 1
group by 
	 e.DateSK,
	 b.ProductSK
	,a.[ClassOfBusinessSK]
	,d.ChannelSK
	,a.BrokerSK
	,a.QuoteCurrencySK 
	,a.UnderwriterSK


--/////////////////////////////////////////Personal Lines Converted Count////////////////////////////////////
drop table if exists [DWH_TMP].[FACT].[transactionPLQuoteConvertedCnt]; -- added amit

select dt2.DateSK
			,cov.[ClassOfBusinessSK]
			,b.ProductSK
			,'0' AS [SubProductSK]
			,b.ChannelSK
			,b.BrokerSK
			,b.PolicyUnderwriterSK
			,b.PolicyCurrencyKey
			,count(distinct b.PolicySK) as 'PLQuoteConvertedCnt' -- COunt of policies
	into [DWH_TMP].[FACT].[transactionPLQuoteConvertedCnt] -- added amit
	from ods.[Policy] b
	join [DWH_TMP].[FACT].MinTran mt
		on b.PolicySK = mt.PolicySK
	join ods.Date dt
		on mt.MinDateSK = dt.DateSK
	join ods.Date dt2  -- get end of month date
		on eomonth(dt.FullDate) = dt2.FullDate
	join ods.[PolicyCoverageBridge] pcb		--Link to coverage bridge
		on b.PolicySK = pcb.policysk
	join ods.[Coverage] cov					--LInk to COverage for CoB
		on pcb.CoverageSk = cov.CoverageSk 
	join ods.ClassOfBusiness cob
		on cob.ClassOfBusinessSK = cov.ClassOfBusinessSK
	where 
		b.PolicyMasterSeq in ('0','1') -- 'New Business' Transaction Type
	and  
		b.PolicyStatusSK in ('1','2','3','4') --New Business, Renewal, MTC, Lapsed
	and 
		b.PolicyStartDtSK != b.PolicyExpiryDtSK -- Remove NTU marked as MTC
	and
		cob.ClassOfBusinessName in ('Direct Household','Direct Motor','Intermediated Household','Intermediated Motor')
	and
		b.[CurrentRow] = 1 
	and
		dt.CurrentRow = 1
	and
		dt2.CurrentRow = 1
	and
		pcb.[CurrentRow] = 1 
	and
		cov.[CurrentRow] = 1
	and
		cob.CurrentRow = 1 
	group by dt2.DateSK
			,cov.[ClassOfBusinessSK]
			,b.ProductSK
			,b.ChannelSK
			,b.BrokerSK
			,b.PolicyUnderwriterSK
			,b.PolicyCurrencyKey;

exec [ODS].[sp_etl_audit_insert_row] '[ODS].[sp_populate_dwh_3_tran_view_sales_and_growth_temp_inserts]','** END **]' 

END


GO



--SQL Advanced Study

/*
Select top 1 * from DIM_MANUFACTURER
Select top 1 * from DIM_MODEL
Select top 1 * from DIM_CUSTOMER
Select top 1 * from DIM_LOCATION
Select top 1 * from DIM_DATE
Select top 1 * from FACT_TRANSACTIONS
*/


--Q1--Begin
		Select l.State as Date from FACT_TRANSACTIONS t
		--left join DIM_CUSTOMER c on c.IDCustomer = t.IDCustomer
		left join DIM_LOCATION l on l.IDLocation = t.IDLocation
		Where Year(t.Date) >= 2005 
		and Year(t.Date) <= (Select max(year(Date)) from FACT_TRANSACTIONS)
		group by l.State
--Q1--End


--Q2--Begin
		Select Country,State,Manufacturer_Name,Sum(T.Quantity) as Quantity_S from FACT_TRANSACTIONS T
		left join DIM_MODEL Mo on T.IDModel = Mo.IDModel
		left join DIM_MANUFACTURER M on Mo.IDManufacturer = M.IDManufacturer
		left join DIM_LOCATION L on L.IDLocation = T.IDLocation
		where Country in ('US') and Manufacturer_Name in ('Samsung')  
		group by Country,State,Manufacturer_Name
		having Sum(T.Quantity) = (Select Max(Quantity_US) from
									(
										Select Country,State,Manufacturer_Name,Sum(T.Quantity) as 'Quantity_US'
										from FACT_TRANSACTIONS T 
										left join DIM_LOCATION L on L.IDLocation = T.IDLocation
										left join DIM_MODEL Mo on T.IDModel = Mo.IDModel
										left join DIM_MANUFACTURER M on Mo.IDManufacturer = M.IDManufacturer
										where Country in ('US') and Manufacturer_Name in ('Samsung')  
										group by Country,State,Manufacturer_Name
									) as a)
		order by Country,State,Manufacturer_Name
--Q2--End
		


--Q3--Begin
		Select distinct IDModel,State,ZipCode, COUNT(quantity) over (partition by IDModel,State,ZipCode) as 'Quantity_ZipCode' 
		from FACT_TRANSACTIONS T
		left join DIM_location L on T.IDLocation = L.IDLocation
		group by IDModel,State,ZipCode,quantity,date
		order by IDModel,State,ZipCode
--Q3--End


--Q4--Begin
		Select T.IDModel,M.Model_Name,TotalPrice from FACT_TRANSACTIONS T
		Left JOIN DIM_Model M on M.IDModel = T.IDModel
		where TotalPrice = 
		(Select min(TotalPrice) 'Cheapest' from
		FACT_TRANSACTIONS)
--Q4--End


--Q5--Begin
WITH Cte_Name_ad as (
		Select Manufacturer_Name,IDModel,Model_Name,Revenue_per_model/Total_Quantity_Model as Average_Per_Model,
		Revenue_per_model,Total_Quantity_Model
		--,Total_Quantity
			--Avg_Per_Manufacturer,Revenue 
		from(
				Select *, DENSE_RANK() over (order by Total_Quantity_manufacturer desc) as Rank_S
				from (
						Select distinct Manufacturer_Name,T.IDModel,Model_Name, 
						--AVG(TotalPrice) over (partition by T.IDModel) Avg_Per_Model
						SuM(Quantity)  over (partition by Manufacturer_Name) Total_Quantity_manufacturer
						,SuM(Quantity)  over (partition by Model_Name) Total_Quantity_Model
						--,AVG(TotalPrice) over (partition by Manufacturer_Name) as Avg_Per_Manufacturer
						,SUM(TotalPrice) over (partition by Model_Name) as Revenue_per_model
						from FACT_TRANSACTIONS T 
						left Join DIM_Model Mo on Mo.IDModel = T.IDModel
						Left join DIM_Manufacturer M on Mo.IDManufacturer = M.IDManufacturer
						Group by Manufacturer_Name,T.IDModel,Model_Name,Quantity,TotalPrice,date
					) as a) as b
		where RANK_S in('1','2','3','4','5')
		group by  Manufacturer_Name,IDModel,Model_Name,Revenue_per_model,Total_Quantity_Model)

	Select * from Cte_Name_ad
	order by Average_Per_Model
--Q5--End



--Q6--BEGIN
With cte_name_2 as(
Select Customer_Name, IDCustomer,Total_price/Total_Counts as Average_Per_Customer from(
	Select distinct Customer_Name, T.IDCustomer,
	(SUM(TotalPrice) over (partition by T.IDCustomer))as Total_price,
	(SUM(Quantity) over (partition by T.IDCustomer)) as Total_Counts
	from FACT_TRANSACTIONS T
	Left join DIM_CUSTOMER C on T.IDCustomer = C.IDCustomer
	where Year(date) = 2009 
	group by Customer_Name, T.IDCustomer,TotalPrice,Quantity) as a)

	Select * from cte_name_2
	where Average_per_Customer > 500
	 	
	--Other menthod
	Select distinct Customer_Name, T.IDCustomer,
	Sum(TotalPrice)/SUM(Quantity) as Average_Per_Customer
	from FACT_TRANSACTIONS T
	Left join DIM_CUSTOMER C on T.IDCustomer = C.IDCustomer
	where Year(date) = 2009 
	group by Customer_Name, T.IDCustomer--,TotalPrice,Quantity
	having 	Sum(TotalPrice)/SUM(Quantity) > 500
  --Q6--END
	
--Q7--BEGIN  
		Select top 5 * from (
		Select IDModel,SUM(Quantity) as TotalQuantity from FACT_TRANSACTIONS
		where Year(Date) in ('2008','2009','2010')
		group by IDModel) as a
		group by IDModel,TotalQuantity
		order by TotalQuantity desc
--Q7--END	


--Q8--BEGIN
		Select Manufacturer_Name,Year_s,TotalSales from (
				Select *,Rank() over (partition by Year_s order by TotalSales desc) as Rank_s
				from (
				Select distinct  Manufacturer_Name,Year(date) as Year_s,
				SUM(CASE when Year(Date) = 2009 then TotalPrice End) over (partition by Manufacturer_Name) as TotalSales
				--SUM(CASE when Year(Date) = 2010 then TotalPrice End) over (partition by Manufacturer_Name) as TotalSales_2010		
				from FACT_TRANSACTIONS T
				left join DIM_MODEL Mo on Mo.IDModel = T.IDModel
				left join DIM_MANUFACTURER M on M.IDManufacturer = Mo.IDManufacturer
				where Year(date) in ('2009')  union all 
				(Select distinct Manufacturer_Name,Year(date) as Year_s,
				SUM(CASE when Year(Date) = 2010 then TotalPrice End) over (partition by Manufacturer_Name) as TotalSales
				--SUM(CASE when Year(Date) = 2010 then TotalPrice End) over (partition by Manufacturer_Name) as TotalSales_2010		
				from FACT_TRANSACTIONS T
				left join DIM_MODEL Mo on Mo.IDModel = T.IDModel
				left join DIM_MANUFACTURER M on M.IDManufacturer = Mo.IDManufacturer
				where Year(date) in ('2010'))) as a
				group by Manufacturer_Name,Year_s,TotalSales
				) as b
		where Rank_s in ('2')
--Q8--END


--Q9--BEGIN
	
				(Select distinct Manufacturer_Name
				from FACT_TRANSACTIONS T
				left join DIM_MODEL Mo on Mo.IDModel = T.IDModel
				left join DIM_MANUFACTURER M on M.IDManufacturer = Mo.IDManufacturer
				where year(date) = 2010)
				Except
				(Select distinct Manufacturer_Name
				from FACT_TRANSACTIONS T
				left join DIM_MODEL Mo on Mo.IDModel = T.IDModel
				left join DIM_MANUFACTURER M on M.IDManufacturer = Mo.IDManufacturer
				where year(date) = 2009)
--Q9--END


--Q10--BEGIN
--Drop table if exists #temporary
	With cte_name_3 as (
	Select T.IDCustomer,Customer_Name, SUM(TotalPrice) TotalSpend from 
	FACT_TRANSACTIONS T
	Left join DIM_CUSTOMER C on T.IDCustomer = C.IDCustomer
	group by T.IDCustomer,Customer_Name
	),

	cte_name_4 as 
	(Select top 100 * from cte_name_3
	order by TotalSpend)

	--Select * from cte_name_4
	Select distinct * into #temporary from(
	Select T.IDCustomer,cte.Customer_Name,Year(date) as Years,
	SUM(TotalPrice) over (partition by Customer_Name,Year(Date)) as TotalSpends,
	(SUM(TotalPrice) over (partition by Customer_Name,Year(Date)))/(SUM(Quantity) over (partition by Customer_Name,Year(Date))) as Avg_Spend,
	AVG(Quantity) over (partition by Customer_Name,Year(Date)) as Avg_Quantity
	from cte_name_4 cte inner join
	FACT_TRANSACTIONS T on cte.IDCustomer = T.IDCustomer
	--Left join DIM_CUSTOMER C on T.IDCustomer = C.IDCustomer
	group by T.IDCustomer,cte.Customer_Name,cte.Customer_Name,Year(Date),TotalPrice,Quantity) as d
	group by IDCustomer,Customer_Name,Years,Avg_Spend,Avg_Quantity,TotalSpends
	order by Avg_Spend desc,Avg_Quantity desc

	--Select * from #temporary where IDCUstomer in ('10003')

	Select IDCustomer,Customer_Name,Years,Avg_Spend,Avg_Quantity,--TotalSpends,PreviousyearSpend,
	ROUND
	(Case when PreviousyearSpend is NULL or PreviousyearSpend  = 0 then 0
	ELSE ((SUM(TotalSpends) over (partition by Customer_Name,Years) - Coalesce(PreviousyearSpend,0))*100)/PreviousyearSpend End,2) as SpendChangePercent
	from
	(Select *,LAG(SUM(TotalSpends)) OVER (Partition by IDCustomer order by YEARS) as PreviousyearSpend
	from #temporary
	group by IDCustomer,Customer_Name,years,Avg_Spend,Avg_Quantity,TotalSpends
	) as c
	--where Customer_Name in ('kallie Blackwood')
	order by IDCustomer,Customer_Name,Years,Avg_Spend,Avg_Quantity



--Q10--END
	
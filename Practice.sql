/*DATA PREPARATION AND UNDERSTANDING

	1. What is the total number of rows in each of the 3 tables in the database?*/
		Select COUNT(*) as 'Count_of_Customer' from Customer;		 --5647
		Select COUNT(*) as 'Count_of_Product' from Product;			 --23
		Select COUNT(*) as 'Count_of_Transaction' from Transactions; --23053

		--select * from Transactions

	--2. What is the total number of transactions that have a return?
		Select Count(*) as 'Total_Returns' 
		from (
		Select Transaction_ID 
		From Transactions
		Group by Transaction_ID
		Having SUM(Total_Amt) = 0
		) as Returned_Product;
		--1942
	
	

	/*3. As you would have noticed, the dates provided across the datasets are not in a correct format.
	As first steps, pis convert the date variables into valid date formats before proceeding ahead.*/
	--DONE	

	/* 4. What is the time range of the transaction data available for analysis?
	Show the output in number of days, months and years simultaneously in different columns. */
		Select *, Day(Tran_date) as [Transaction Day],
				 Month(Tran_date) as [Transaction Month],
				 YEAR(Tran_date) as [Transaction Year]
		from Transactions

	--5. Which product category does the sub-category "DIY" belong to?

		Select * from Product
		where Prod_Subcat = 'DIY' --Books


/*DATA ANALYSIS
1. Which channel is most frequently used for transactions?*/
		Select  Store_Type,Frequency_Channel  
		from
			(Select Store_Type,Count(Store_Type) Frequency_Channel,
				Rank() over (order by COUNT(Store_Type) desc) as Rank_s
				from transactions
				group by Store_Type
			) as j
		where Rank_s = 1

--2. What is the count of Male and Female customers in the database?
			Select Count(c.Customer_ID) as Count_of_Females 
			from Customer c
			where Gender = 'F'

			Select Count(c.Customer_ID) as Count_of_Males 
			from Customer c
			where Gender = 'M'

--3. From which city do we have the maximum number of customers and how many?
		Select City_Code,NumberOfCustomers
		from
			(Select City_Code,Rank() over (order by Count(City_Code) desc) as Rank_s,Count(City_Code) as NumberOfCustomers
			from Customer
			group by City_Code
			) as a
		where rank_s = 1


--4. How many sub-categories are there under the Books category?
		Select Prod_Cat,Count(Prod_Subcat) as CountOfSubcat from Product 
		where Prod_Cat = 'Books'
		group by Prod_Cat

--5. What is the maximum quantity of products ever ordered?
		Select p.prod_cat,t.ProductSold from product p
		left join
			(Select Prod_Cat_code,Count(Prod_Cat_code) ProductSold, Rank() over (order by Count(Prod_Cat_code) desc) as rank_s
			from transactions
			group by Prod_Cat_code
			) as t on p.Prod_cat_code = t.Prod_Cat_Code
		where rank_s = 1
		group by p.prod_cat,t.ProductSold

--6. What is the net total revenue generated in categories Electronics and Books?
--1st method
		Select  t.Prod_Cat_Code, Sum(t.Total_Amt) as TotalRevenue  from product p
		right join transactions t on t. Prod_Cat_Code = p.Prod_Cat_Code and t.Prod_Subcat_Code = p.prod_Sub_cat_code
		where p.Prod_Cat in ('Electronics', 'Books')
		group by t.Prod_Cat_Code--,prod_Sub_cat_code
		
		--2nd method
		Select   Sum(t.Total_Amt) as TotalRevenue  from product p
		right join transactions t on t. Prod_Cat_Code = p.Prod_Cat_Code and t.Prod_Subcat_Code = p.prod_Sub_cat_code
		where p.Prod_Cat in ('Electronics', 'Books')

--7. How many customers have >10 transactions with us, excluding returns?
		Select Customer_Id, Sum(Spent) as SpentMoney, Sum(Purchases) as TotalTransactions from (
		Select  t.Customer_Id, count(t.Transaction_Id)-- over (partition by t.Customer_Id) 
		as Purchases,
		Sum(t.Total_Amt) as Spent
		from Customer c 
		right join Transactions t on c.Customer_ID = t.Customer_Id
		group by t.Customer_Id,t.Transaction_Id
		having Sum(t.Total_Amt) <> 0
		) as j
		group by Customer_Id
		having Sum(Purchases) > 10

	


--8. What is the combined revenue earned from the "Electronics" & "Clothing"
--categories, from "Flagship stores"?
		Select Sum(Total_Amt) Revenue  from Transactions t
		left join Product p 
		on t.Prod_Cat_Code =p.Prod_Cat_Code 
		and t.Prod_Subcat_Code = p.Prod_Sub_Cat_Code
		where Store_Type in ('Flagship store') 
		and p.Prod_Cat in ('Electronics','Clothing')
		--group by t.Prod_Cat_Code,p.Prod_Cat

--9. What is the total revenue generated from "Male" customers in "Electronics"
--* category? Output should display total revenue by prod sub-cat.
		Select p.Prod_Subcat,Sum(Total_Amt) Revenue from transactions t
		left join Customer c on t.Customer_Id = c.Customer_ID
		left join Product p on p.Prod_Cat_Code = t.Prod_Cat_Code and p.Prod_Sub_Cat_Code = t.Prod_Subcat_Code
		where Gender ='M' and p.Prod_Cat in ('Electronics')
		group by p.Prod_Subcat

--10. What is percentage of sales and returns by product sub category; display only top
--5 sub categories in terms of sales?
		
		/*Select  p.Prod_Subcat,p.Prod_Sub_Cat_Code,SUM(Coalesce(Returncounts,0)) as ReturnCount,
		SUM(Coalesce(Salecounts,0)) as SaleCounts,
		SUM(Coalesce(Returncounts,0)) + SUM(Coalesce(Salecounts,0)) as TotalCount 
		--(SUM(Coalesce(Returncounts,0)) * 100)/(SUM(Coalesce(Returncounts,0)) + SUM(Coalesce(Salecounts,0))) as PercentOfReturn,
		--(SUM(Coalesce(Salecounts,0)) * 100)/(SUM(Coalesce(Returncounts,0)) + SUM(Coalesce(Salecounts,0))) as PercentOfSale
		from Product p right join(
		Select Prod_Subcat_Code,
		Case when Sum(Total_Amt) = 0 then Count(*) End as Returncounts,
		Case when Sum(Total_Amt) <> 0 then count(*) End as Salecounts
		--Count(*) as Counts
		from Transactions
		group by Prod_Subcat_Code,Customer_Id
		order by
		) as j on p.Prod_Sub_Cat_Code = j.Prod_Subcat_Code and p.Prod_Cat_Code = j.Prod_Cat_Code
		group by p.Prod_Subcat,p.Prod_Sub_Cat_Code
		--order by Prod_Subcat_Code*/
Drop table if exists #temp
				
		With cte_name as (
		Select distinct Transaction_Id,Prod_Subcat_Code,
		--SUM(Case when Sum(Total_Amt) = 0 then Count(Total_Amt) End ) over (partition by Transaction_Id)  as Returncounts,
		Sum(Case when Sum(Total_Amt) <> 0 then count(Total_Amt) End) over (partition by Transaction_Id) as Salecounts
		,Sum(Total_Amt) over (partition by Transaction_Id) as TA
		--Count(*) as Counts
		from Transactions
		--where Prod_Subcat_Code in ('9') and Transaction_Id in ('2352870280','1891510931','89172283224')
		group by Transaction_Id, Prod_Subcat_Code,Total_Amt,Tran_Date
		)

	Select  Prod_Subcat_Code,Convert(Decimal(10,2),(Return_s * 100.00)/(Return_s+Sold))   as ReturnPercentage,
	Convert(Decimal(10,2),(Sold * 100.00)/(Return_s+Sold)) as Salepercentage
	into #temp
	from(
	Select Prod_Subcat_Code,Sum(Coalesce(return_s,0)) as Return_s,Sum(Coalesce(Sold,0)) as Sold 
	from
	(Select Prod_Subcat_Code,Case when Salecounts > 1 then Count(Salecounts) End as return_s,
	Case when Salecounts = 1 then Count(Salecounts) End as Sold
	from cte_name	
	--where Salecounts > 1
	group by Prod_Subcat_Code,Salecounts) as a
	group by Prod_Subcat_Code) as b	

	Select Prod_Subcat_Code,ReturnPercentage,Salepercentage from #temp
	
	Select top 5 * from #temp 
	order by Salepercentage desc

		

--11. For all customers aged between 25 to 35 years find what is the net total revenue generated by thesg consumers
--in last 30 days of transactions from max transaction date available in the data?

	With cte as (Select * from (
	Select *,DATEDIFF(year,DOB,GETDATE()) as Age from Customer) as a
	where Age > 25 and Age < 35)
	--where age between 25 and 35

	Select t.Customer_ID,SUM(Total_Amt) as Revenue from cte c
	left join Transactions t on c.Customer_ID =t.Customer_Id
	group by t.Customer_Id


--12. Which product category has seen the max value of returns in the last 3 months of transactions?
		/* With cte_name_s as (
		Select distinct Transaction_Id,prod_cat_code,
		--SUM(Case when Sum(Total_Amt) = 0 then Count(Total_Amt) End ) over (partition by Transaction_Id)  as Returncounts,
		Sum(Case when Sum(Total_Amt) <> 0 then count(Total_Amt) End) over (partition by Transaction_Id) as Salecounts
		,Sum(Total_Amt) over (partition by Transaction_Id) as TA
		--Count(*) as Counts
		from Transactions
		--where Prod_Subcat_Code in ('9') and Transaction_Id in ('2352870280','1891510931','89172283224')
		group by Transaction_Id, prod_cat_code,Total_Amt,Tran_Date
		)

	Select Top 1 prod_cat_code,Convert(Decimal(10,2),(Return_s * 100.00)/(Return_s+Sold))   as ReturnPercentage,
	Convert(Decimal(10,2),(Sold * 100.00)/(Return_s+Sold)) as Salepercentage
	into #temps
	from(
	Select prod_cat_code,Sum(Coalesce(return_s,0)) as Return_s,Sum(Coalesce(Sold,0)) as Sold 
	from
	(Select prod_cat_code,Case when Salecounts > 1 then Count(Salecounts) End as return_s,
	Case when Salecounts = 1 then Count(Salecounts) End as Sold
	from cte_name_s	
	--where Salecounts > 1
	group by prod_cat_code,Salecounts) as a
	group by prod_cat_code) as b	
	order by salepercentage desc
	
	Select p.Prod_Cat,t.prod_cat_code,Returnpercentage,Salepercentage
	from product p right join #temps t on t.prod_cat_code = p.prod_cat_code
	group by p.Prod_Cat,t.prod_cat_code,Returnpercentage,Salepercentage */

	Drop table if exists #temp1

	Select distinct Transaction_Id,prod_cat_code,SUM(Total_Amt) over(partition by Transaction_Id) as Revenue 
	--into #temp1
	from transactions
	where tran_date >= DATEADD(MONTH,-3,(Select max(Tran_Date) from Transactions))
	group by Transaction_Id,prod_cat_code,Total_Amt--,Tran_Date
	--order by tran_date desc

	Select Prod_Cat,a.Prod_Cat_Code,a.TotalReturns from Product p 
	inner join(
	Select distinct top 1 prod_cat_code, count(Revenue) over (partition by prod_cat_code) as TotalReturns from #temp1
	where Revenue <= 0 order by TotalReturns desc
	) as a on p.prod_cat_code = a.prod_cat_code
	group by Prod_Cat,a.Prod_Cat_Code,a.TotalReturns

--13. Which store-type sells the maximum products; by value of sales amount and by quantity sold?
	--By Revenue
	Select top 1 Store_Type,
	Count(Store_Type) as QuantitySold,
	Sum(Total_Amt) as Revenue 
	from Transactions
	group by Store_Type
	order by Revenue desc

	--By QuantitySold
	Select top 1 Store_Type,
	Count(Store_Type) as QuantitySold,
	Sum(Total_Amt) as Revenue 
	from Transactions
	group by Store_Type
	order by QuantitySold desc

--14. What are the categories for which average revenue is above the overall average.
	Select * from
	(Select distinct Prod_Cat_Code,AVG(Total_Amt) over (partition by Prod_Cat_Code) as AveragePerCat
	--,Avg(Total_Amt) as OverallAverage	
	from Transactions
	group by Prod_Cat_Code, Total_Amt,Tran_Date ) as a
	where AveragePerCat > (Select AVG(Total_Amt) as AVG_S from Transactions) 


--15. Find the average and total revenue by each subcategory for the categories which are among top 5 categories 
--in terms of quantity sold.
	/*Select  prod_subcat_code,AVG(Total_Amt) over (partition by prod_subcat_code) as AveragePerSubCat,
	Sum(Total_Amt) over (partition by prod_subcat_code) as TotalRevenuePerSubCat,
	count(prod_subcat_code) over (partition by prod_subcat_code) as count_s,
	count(transaction_id) over (partition by transaction_id) as ct, Transaction_Id
	--,Avg(Total_Amt) as OverallAverage	
	from Transactions
	group by prod_subcat_code, Tran_Date,Total_Amt,transaction_id
	--having SUM(Total_Amt) > 0
	order by count_s asc*/

	Select distinct top 5 prod_subcat_code,
	AVG(Sumtransactions) over (partition by prod_subcat_code) as AveragePerSubCat,
	Sum(Sumtransactions) over (partition by prod_subcat_code) as TotalRevenuePerSubCat ,
	Count(Sumtransactions) over (partition by prod_subcat_code) as Quantity
	from
	(Select * from 
	(Select transaction_id,prod_subcat_code,
	count(transaction_id) over (partition by transaction_id) as counttransactions,
	Sum(Total_Amt) over (partition by transaction_id) as Sumtransactions
	from Transactions
	group by transaction_id,prod_subcat_code,Tran_Date,Total_Amt) as a
	where Sumtransactions > 0) as b
--	where prod_subcat_code = 4
	group by transaction_id,prod_subcat_code,Sumtransactions,counttransactions
	order by Quantity desc

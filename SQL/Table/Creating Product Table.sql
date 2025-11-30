--Drop table if exists Product

--Creating Table
		Create table Product
		(Prod_Cat_Code int,
		 Prod_Cat varchar(20),
		 Prod_Sub_Cat_Code int,
		 Prod_Subcat varchar(50)
		);

--Selecting Record
		Select * from Product 


--Inserting Values in table
		INSERT INTO Product Values (1,'Clothing',4,'Mens')
		INSERT INTO Product Values (1,'Clothing',1,'Women')
		INSERT INTO Product Values (1,'Clothing',3,'Kids')
		INSERT INTO Product Values (2,'Footwear',1,'Mens')
		INSERT INTO Product Values (2,'Footwear',3,'Women')
		INSERT INTO Product Values (2,'Footwear',4,'Kids')
		INSERT INTO Product Values (3,'Electronics',4,'Mobiles')
		INSERT INTO Product Values (3,'Electronics',5,'Computers')
		INSERT INTO Product Values (3,'Electronics',8,'Personal Appliances')
		INSERT INTO Product Values (3,'Electronics',9,'Cameras')
		INSERT INTO Product Values (3,'Electronics',10,'Audio and video')
		INSERT INTO Product Values (4,'Bags',1,'Mens')
		INSERT INTO Product Values (4,'Bags',4,'Women')
		INSERT INTO Product Values (5,'Books',7,'Fiction')
		INSERT INTO Product Values (5,'Books',12,'Academic')
		INSERT INTO Product Values (5,'Books',10,'Non-Fiction')
		INSERT INTO Product Values (5,'Books',11,'Children')
		INSERT INTO Product Values (5,'Books',3,'Comics')
		INSERT INTO Product Values (5,'Books',6,'DIY')
		INSERT INTO Product Values (6,'Home and kitchen',2,'Furnishing')
		INSERT INTO Product Values (6,'Home and kitchen',10,'Kitchen')
		INSERT INTO Product Values (6,'Home and kitchen',11,'Bath')
		INSERT INTO Product Values (6,'Home and kitchen',12,'Tools')



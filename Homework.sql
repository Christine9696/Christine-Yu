
--StockItems that are sold to customers in MD or VA
SELECT DISTINCT StockItemName
FROM Warehouse.StockItems
WHERE StockItemID NOT IN(
SELECT DISTINCT s1.StockItemID
from Warehouse.Stockitems s1
     JOIN
     sales.orderlines s2
     ON s1.StockItemID = s2.StockItemID
     JOIN sales.Orders o
     ON s2.OrderID = o.OrderID
     JOIN
     Sales.Customers c
     ON o.CustomerID = c.CustomerID
     JOIN Application.cities ct
     ON ct.CityID=c.PostalCityID
     JOIN Application.StateProvinces c2
     ON ct.StateProvinceID = c2.StateProvinceID AND  c2.StateProvinceCode IN ('VA','MD'))
---stockitems that are not sold to customers in MD or VA
SELECT DISTINCT s1.StockItemName
from Warehouse.Stockitems s1
     JOIN
     sales.orderlines s2
     ON s1.StockItemID = s2.StockItemID
     JOIN sales.Orders o
     ON s2.OrderID = o.OrderID
     JOIN
     Sales.Customers c
     ON o.CustomerID = c.CustomerID
     JOIN Application.cities ct
     ON ct.CityID=c.PostalCityID
     JOIN Application.StateProvinces c2
     ON ct.StateProvinceID = c2.StateProvinceID AND  c2.StateProvinceCode IN ('VA','MD')
---Customer cities and total kinds of stockitems purchased
SELECT c.CityName,count(DISTINCT st.StockItemID) kinds
FROM Application.Cities c
     JOIN
     Sales.Customers c2
     ON c.CityID = c2.PostalCityID
     JOIN
     Warehouse.StockItemTransactions st
     ON c2.CustomerID = st.CustomerID
GROUP BY c.CityName

--Customer cities and 2nd most sold stock item
select a.CityName, a.StockItemName, a.rank
from (
select ct.CityName, stockitemname,  ROW_NUMBER() over (partition by ct.cityname order by sum(ol.quantity) desc) rank from application.Cities ct
join sales.Customers c on ct.CityID=c.PostalCityID
join sales.Orders o on o.CustomerID=c.CustomerID
join sales.OrderLines ol on o.OrderID=ol.OrderID
join Warehouse.StockItems s on s.StockItemID=ol.StockItemID
group by ct.CityName, StockItemName) a
where rank=2

--StockItems purchased more than sold in 2015
select s1.StockItemName
From Warehouse.StockItems s1
     JOIN
     (SELECT p1.StockItemID, sum(p1.OrderedOuters) total
      FROM Warehouse.StockItems s
            JOIN
            Purchasing.PurchaseOrderLines p1
            ON s.StockItemID = p1.StockItemID
            JOIN
            Purchasing.PurchaseOrders p2
            ON p1.PurchaseOrderID = p2.PurchaseOrderID
      WHERE YEAR(p2.OrderDate) = '2015'
      GROUP BY p1.StockItemID ) a ON s1.StockItemID = a.StockItemID
      LEFT JOIN
      (SELECT ol.StockItemID, sum(ol.Quantity) total1
      FROM Warehouse.StockItems s
            JOIN
            Sales.OrderLines ol
            ON s.StockItemID = ol.StockItemID
            JOIN
            Sales.Orders o
            ON ol.OrderID = o.OrderID
      WHERE YEAR(o.OrderDate) = '2015'
      GROUP BY ol.StockItemID) b ON s1.StockItemID = b.StockItemID
WHERE a.total > b.total1 or b.total1 is null
---SOLUTION
select s.StockItemName from Warehouse.stockitems s
join
(select s.StockItemID , sum(pol.OrderedOuters) quantity from Warehouse.StockItems s
join Purchasing.PurchaseOrderLines pol on s.StockItemID=pol.StockItemID
join Purchasing.PurchaseOrders po on po.PurchaseOrderID = pol.PurchaseOrderID
where YEAR(po.OrderDate)=2015
group by s.StockItemID ) a on a.StockItemID = s.StockItemID
left join (
select s.StockItemID , sum(pol.Quantity) quantity from Warehouse.StockItems s
join sales.OrderLines pol on s.StockItemID=pol.StockItemID
join sales.orders po on po.OrderID = pol.OrderID
where YEAR(po.OrderDate)=2015
group by s.StockItemID
) b on a.StockItemID=b.StockItemID
where a.quantity>b.quantity or b.quantity is null


--Customers who bought mugs but not toys
select distinct c.CustomerName from sales.Customers c
join sales.Orders o on c.CustomerID=o.CustomerID
join sales.OrderLines ol on o.OrderID=ol.OrderID
join Warehouse.StockItems s on s.StockItemID=ol.StockItemID
join Warehouse.StockItemStockGroups sisg on s.StockItemID=sisg.StockItemID
join Warehouse.StockGroups sg on sisg.StockGroupID=sg.StockGroupID
where sg.StockGroupName ='mugs'
except
select distinct c.CustomerName from sales.Customers c
join sales.Orders o on c.CustomerID=o.CustomerID
join sales.OrderLines ol on o.OrderID=ol.OrderID
join Warehouse.StockItems s on s.StockItemID=ol.StockItemID
join Warehouse.StockItemStockGroups sisg on s.StockItemID=sisg.StockItemID
join Warehouse.StockGroups sg on sisg.StockGroupID=sg.StockGroupID
where sg.StockGroupName ='toys'
----Practice
SELECT c.CustomerName
FROM Sales.Customers c
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
     JOIN
     Sales.OrderLines ol
     ON o.OrderID  = ol.OrderID
     JOIN
     Warehouse.StockItems s
     ON ol.StockItemID = s.StockItemID
     JOIN
     Warehouse.StockItemStockGroups s2
     ON s.StockItemID = s2.StockItemID
     JOIN
     Warehouse.StockGroups s3
     ON s2. StockGroupID = s3.StockGroupID
WHERE s3.StockGroupName = 'Mugs'
EXCEPT
SELECT c.CustomerName
FROM Sales.Customers c
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
     JOIN
     Sales.OrderLines ol
     ON o.OrderID  = ol.OrderID
     JOIN
     Warehouse.StockItems s
     ON ol.StockItemID = s.StockItemID
     JOIN
     Warehouse.StockItemStockGroups s2
     ON s.StockItemID = s2.StockItemID
     JOIN
     Warehouse.StockGroups s3
     ON s2. StockGroupID = s3.StockGroupID
WHERE S3.StockGroupName='Toys'

----Solution1
select w1.StockItemName
from Warehouse.StockItems w1 join Warehouse.StockItemTransactions w2 on w1.StockItemID=w2.StockItemID
where YEAR(w2.TransactionOccurredWhen)=2015
group by w1.StockItemName
having SUM(w2.Quantity)>0;
---Solution2
select s1.StockItemName
From Warehouse.StockItems s1
     JOIN
     (SELECT p1.StockItemID, sum(p1.OrderedOuters) total
      FROM Warehouse.StockItems s
            JOIN
            Purchasing.PurchaseOrderLines p1
            ON s.StockItemID = p1.StockItemID
            JOIN
            Purchasing.PurchaseOrders p2
            ON p1.PurchaseOrderID = p2.PurchaseOrderID
      WHERE YEAR(p2.OrderDate) = '2015'
      GROUP BY p1.StockItemID ) a ON s1.StockItemID = a.StockItemID
      LEFT JOIN
      (SELECT ol.StockItemID, sum(ol.Quantity) total1
      FROM Warehouse.StockItems s
            JOIN
            Sales.OrderLines ol
            ON s.StockItemID = ol.StockItemID
            JOIN
            Sales.Orders o
            ON ol.OrderID = o.OrderID
      WHERE YEAR(o.OrderDate) = '2015'
      GROUP BY ol.StockItemID) b ON s1.StockItemID = b.StockItemID
WHERE a.total > b.total1 or b.total1 is null
-------1
SELECT c.CustomerName, c.FaxNumber,c.PhoneNumber,s.SupplierName, s.FaxNumber, s.PhoneNumber
FROM Sales.Customers c
     LEFT JOIN
     Purchasing.Suppliers s
     ON c.FaxNumber = s.FaxNumber
----2

WITH t1 AS (
SELECT DISTINCT a.CustomerID, a.PhoneNumber
FROM Sales.Customers a
      JOIN
      (SELECT PrimaryContactPersonID,PhoneNumber
       FROM Sales.Customers
      ) b
      ON a.PhoneNumber = b.PhoneNumber
)
SELECT s.SupplierName,t1.CustomerID,t1.PhoneNumber
FROM t1
     JOIN
     Purchasing.Suppliers s
     ON t1.CustomerID = s.PrimaryContactPersonID

---3
SELECT c.CustomerName, o.OrderDate
FROM Sales.Customers c
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
WHERE YEAR(o.OrderDate)< 2016
EXCEPT
SELECT c.CustomerName, o.OrderDate
FROM Sales.Customers c
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
WHERE o.OrderDate > '2016-01-01'
---4
SELECT s.StockItemName, sum(ol.Quantity) total
FROM Sales.Orders o
     JOIN
     Sales.OrderLines ol
     ON o.OrderID = ol.OrderID
     JOIN
     Warehouse.Stockitems s
     ON ol.StockItemID = s.StockItemID
WHERE YEAR(o.OrderDate) = 2013
GROUP BY s.StockItemName
---5(Not sure)
SELECT DISTINCT s.StockItemName, LEN(s.SearchDetails) AS Length
FROM Warehouse.StockItems s
     JOIN
     Sales.InvoiceLines i
     ON s.StockItemID = i.StockItemID
WHERE LEN(i.Description) > 10
---6
SELECT DISTINCT s1.StockItemName
FROM Warehouse.StockItems s1
     JOIN
     Sales.OrderLines ol
     ON s1.StockItemID = ol.StockItemID
    JOIN
    Sales.Orders o
    ON ol.OrderID = o.OrderID
    JOIN
    Sales.Customers c
    ON o.CustomerID = c.CustomerID
    JOIN
    Application.Cities c2
    ON c.PostalCityID = c2.CityID
    JOIN
    Application.StateProvinces s2
    ON c2.StateProvinceID = s2.StateProvinceID
WHERE s2.StateProvinceCode NOT IN ('AL','GA') AND Year(o.OrderDate)=2014
---7

SELECT s2.StateProvinceName, avg(DATEDIFF(day,o.OrderDate,i.ConfirmedDeliveryTime)) AS AVG_Processing_TIme
FROM Sales.invoices i
     JOIN
     Sales.Orders o
     ON i.OrderID = o.OrderID
     JOIN
     Sales.Customers c
     ON o.CustomerID = c.CustomerID
     JOIN
     Application.Cities c2
     ON c.PostalCityID = c2.CityID
     JOIN
     Application.StateProvinces s2
     ON c2.StateProvinceID = s2.StateProvinceID
GROUP BY s2.StateProvinceName

---8
SELECT s2.StateProvinceName, avg(DATEDIFF(month,o.OrderDate,i.ConfirmedDeliveryTime)) AS AVG_time_month
FROM Sales.invoices i
     JOIN
     Sales.Orders o
     ON i.OrderID = o.OrderID
     JOIN
     Sales.Customers c
     ON o.CustomerID = c.CustomerID
     JOIN
     Application.Cities c2
     ON c.PostalCityID = c2.CityID
     JOIN
     Application.StateProvinces s2
     ON c2.StateProvinceID = s2.StateProvinceID
GROUP BY s2.StateProvinceName
---9
select s.StockItemName from Warehouse.stockitems s
join
(select s.StockItemID , sum(pol.OrderedOuters) quantity from Warehouse.StockItems s
join Purchasing.PurchaseOrderLines pol on s.StockItemID=pol.StockItemID
join Purchasing.PurchaseOrders po on po.PurchaseOrderID = pol.PurchaseOrderID
where YEAR(po.OrderDate)=2015
group by s.StockItemID ) a on a.StockItemID = s.StockItemID
left join (
select s.StockItemID , sum(pol.Quantity) quantity from Warehouse.StockItems s
join sales.OrderLines pol on s.StockItemID=pol.StockItemID
join sales.orders po on po.OrderID = pol.OrderID
where YEAR(po.OrderDate)=2015
group by s.StockItemID
) b on a.StockItemID=b.StockItemID
where a.quantity>b.quantity or b.quantity is null
---10 List of Customers and their phone number, together with the primary contact person’s name,
---- to whom we did not sell more than 10  mugs (search by name) in the year 2016.


SELECT c.CustomerName,c.PhoneNumber,p.FullName
FROM Application.People p
     JOIN
     Sales.Customers c
     ON p.PersonID = c.PrimaryContactPersonID
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
     JOIN
     Sales.OrderLines ol
     ON ol.OrderID = o.OrderID
     JOIN
     Warehouse.StockItemStockGroups t2
     ON ol.StockItemID = t2.StockItemID
     JOIN
     Warehouse.StockGroups t3
     ON t2.StockGroupId = t3.StockGroupID
EXCEPT
SELECT c.CustomerName,c.PhoneNumber,p.FullName
FROM Application.People p
     JOIN
     Sales.Customers c
     ON p.PersonID = c.PrimaryContactPersonID
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
     JOIN
     Sales.OrderLines ol
     ON ol.OrderID = o.OrderID
     JOIN
     Warehouse.StockItemStockGroups t2
     ON ol.StockItemID = t2.StockItemID
     JOIN
     Warehouse.StockGroups t3
     ON t2.StockGroupId = t3.StockGroupID
WHERE t3.StockGroupName = 'Mugs' AND YEAR(o.OrderDate)=2016
GROUP BY c.CustomerName,c.PhoneNumber,p.FullName
HAVING(sum(ol.Quantity)<=10)
---11
SELECT DISTINCT CityName
FROM WideWorldImporters.Application.Cities
    for system_time between '2015-01-01' and '9999-12-31'
---12
select s.StockItemName, concat(c.DeliveryAddressLine1,', ',c.DeliveryAddressLine2) DeliveryAddress, ct.CityName, co.CountryName,
        c.PhoneNumber, isnull(temp.quantity,0) Quantity
from Warehouse.StockItems s
join sales.OrderLines ol on ol.StockItemID=s.StockItemID
join sales.Orders o on ol.OrderID = o.OrderID
join sales.Customers c on o.CustomerID=c.CustomerID
join Application.People p on c.PrimaryContactPersonID=p.PersonID
join Application.Cities ct on ct.CityID=c.PostalCityID
join Application.StateProvinces sp on sp.StateProvinceID=ct.StateProvinceID
join Application.Countries co on co.CountryID=sp.CountryID
left join (
    select stockItemName, count(*) as quantity
    from Purchasing.PurchaseOrderLines pol
    join Warehouse.StockItems si on pol.stockItemID=si.StockItemID
    group by stockItemName

) temp on temp.StockItemName=s.StockItemName
where o.OrderDate='2014-07-01'

--13
SELECT s3.StockGroupName, sum(cast(p.OrderedOuters as bigint)) as quantity_purchased , sum(cast(ol.quantity as bigint)) as quantity_sold, (sum(cast(p.OrderedOuters as bigint)) - sum(cast(ol.Quantity as bigint))) as remaining
FROM Purchasing.PurchaseOrderLines p
     JOIN
     Sales.OrderLines ol
     ON p.StockItemID = ol.StockItemID
     JOIN
     Warehouse.StockItems s
     ON ol.StockItemID = s.StockItemID
     JOIN
     Warehouse.StockItemStockGroups s2
     ON s.StockItemID = s2.StockItemID
     JOIN
     Warehouse.StockGroups s3
     ON s2.StockGroupID = s3.StockGroupID
GROUP BY s3.StockGroupName


--14.List of Cities in the US and the stock item that the city got the most deliveries in 2016.
---If the city did not purchase any stock items in 2016, print “No Sales”.
select DISTINCT CityName, ISNULL(StockItemName,'No Sales') as StockItemName, isnull(Quantity,0) as Quantity,rank
from(
select c2.CityName, s2.StockItemName, sum(ol.Quantity) as Quantity,
DENSE_RANK()over(partition by c2.CityName order by sum(ol.Quantity) desc) as rank
from Application.Countries c
     JOIN
     Application.StateProvinces s
     ON c.CountryID = s.CountryID
     JOIN
     Application.Cities c2
     ON s.StateProvinceID = c2.StateProvinceID
     LEFT JOIN Sales.Customers c3 ON C3.DeliveryCityID=c2.CityID
     LEFT JOIN Sales.Orders o ON c3.CustomerID=o.CustomerID
     LEFT JOIN Sales.OrderLines ol ON o.OrderID=ol.OrderID
     LEFT JOIN Warehouse.StockItems s2 ON ol.StockItemID = s2.StockItemID
WHERE c.IsoAlpha3Code = 'USA' and YEAR(o.OrderDate) = 2016
group by c2.CityName, s2.StockItemName) a
where rank=1


---15
SELECT o.OrderID,i.ReturnedDeliveryData
FROM Sales.Orders o
     JOIN
     sales.Invoices i
     ON o.OrderID = i.OrderID
WHERE JSON_VALUE(i.ReturnedDeliveryData, '$.Events[1].Comment') = 'Receiver not present'
---16.  List all stock items that are manufactured in China. (Country of Manufacture)

SELECT DISTINCT s.StockItemName
FROM Warehouse.StockItems s
WHERE JSON_VALUE(s.CustomFields,'$.CountryOfManufacture') = 'China'

---17(how to make it show 'no sales' when the value is null?)
SELECT [China],[Japan],[USA]
FROM (
SELECT JSON_VALUE(s.CustomFields,'$.CountryOfManufacture') Country,ol.Quantity
FROM Warehouse.StockItems s
     JOIN
     Sales.OrderLines ol
     ON s.StockItemID = ol.StockItemID
     JOIN
     Sales.Orders o
     ON ol.OrderID = o.OrderID
WHERE YEAR(o.OrderDate) = 2015
) a
PIVOT
(sum(Quantity)
 FOR Country IN ([China],[Japan],[USA])
)AS PivotTable



--20.   Create a function, input: order id; return: total of that order.
----    List invoices and use that function to attach the order total to the other fields of invoices.

DROP function if exists Sales.function1
GO
CREATE FUNCTION Sales.function1 (@OrderID int)
RETURNS int
AS
BEGIN
    DECLARE @total int
    SELECT @total = COUNT(ol.Quantity)
    FROM Sales.Orders o
         JOIN
         Sales.OrderLines ol
         ON
         o.OrderID = ol.OrderID
    WHERE ol.OrderID = @OrderID
    GROUP BY ol.Quantity
    RETURN @total;
END;

---21 Create a new table called ods.Orders. Create a stored procedure,
--- with proper error handling and transactions, that input is a date; when executed,
---it would find orders of that day, calculate order total, and save the information (order id, order date, order total, customer id)
--- into the new table. If a given date is already existing in the new table, throw an error and roll back.
---- Execute the stored procedure 5 times using different dates.

Go
CREATE SCHEMA ods
Drop table if exists ods.ORDERS;
Go
CREATE TABLE ods.ORDERS(OrderID int,OrderDate date,OrderTotal int, CustomerID int);

DROP PROCEDURE IF EXISTS procedure1;
GO
CREATE PROCEDURE procedure1 (@date date)
AS BEGIN
BEGIN TRY
Begin Transaction OrderDateTotal
IF @date IN(
        SELECT DISTINCT OrderDate
        FROM ods.Orders)
   ROLLBACK
ELSE
INSERT INTO ods.Orders (OrderID,OrderDate,OrderTotal,CustomerID)
SELECT OrderID,OrderDate,OrderTotal,CustomerID
FROM(
SELECT o.OrderID,o.OrderDate,SUM(ol.Quantity) as OrderTotal,o.CustomerID
FROM Sales.Orders o
     JOIN
     Sales.OrderLines ol
     ON o.OrderID = ol.OrderID
WHERE @date = o.OrderDate
GROUP BY o.OrderDate,o.OrderID,o.CustomerID) a
COMMIT Transaction OrderDateTotal
END TRY
BEGIN CATCH
IF @@TRANCOUNT > 0
Rollback Transaction OrderDateTotal;
END CATCH
END;
GO

     EXEC dbo.procedure1 @date= '2013-01-10';
     EXEC dbo.procedure1 @date= '2013-01-01';
     EXEC dbo.procedure1 @date= '2013-01-11';
     EXEC dbo.procedure1 @date= '2013-01-13';
     EXEC dbo.procedure1 @date= '2013-01-15';



SELECT *
FROM ods.ORDERS

---practice
WITH a AS(
select s.StockItemName, s.StockItemID , sum(pol.OrderedOuters) quantity
from Warehouse.StockItems s
join Purchasing.PurchaseOrderLines pol on s.StockItemID=pol.StockItemID
join Purchasing.PurchaseOrders po on po.PurchaseOrderID = pol.PurchaseOrderID
where YEAR(po.OrderDate)=2015
group by s.StockItemName, s.StockItemID ),b as(
select s.StockItemName,s.StockItemID , sum(pol.Quantity) quantity from Warehouse.StockItems s
join sales.OrderLines pol on s.StockItemID=pol.StockItemID
join sales.orders po on po.OrderID = pol.OrderID
where YEAR(po.OrderDate)=2015
group by s.StockItemName, s.StockItemID
)
SELECT a.StockItemName
FROM a
     LEFT JOIN b
     ON a.StockItemID = b.StockItemID
where a.quantity>b.quantity or b.quantity is null
----22
Drop Table if exists ods.StockItem;
CREATE TABLE ods.StockItem (
    StockItemID int not null,
    StockItemName nvarchar(100) not null,
    SupplierID int not null,
    ColorID int null,
    UnitPackageID int not null,
    OuterPackageID int not null,
    Brand nvarchar(50) null,
    Size nvarchar(20) null,
    LeadTimeDays int not null,
    QuantityPerOuter int not null,
    IsChillerStock bit not null,
    Barcode nvarchar(50) null ,
    TaxRate decimal(18,3) not null,
    UnitPrice decimal(18,2) not null,
    RecommendedRetailPrice decimal(18,2) null ,
    TypicalWeightPerUnit decimal(18,3) not null,
    MarketingComments nvarchar(max) null,
    InternalComments nvarchar(max) null,
     CountryOfManufacture nvarchar(max) NULL,
     Range nvarchar(max) NULL ,
     Shelflife nvarchar(max) NULL
  CONSTRAINT [PK_ods.StockItem] PRIMARY KEY CLUSTERED(
  StockItemID ASC)
     )

INSERT INTO ods.StockItem (StockItemID,
    StockItemName,
    SupplierID,
    ColorID,
    UnitPackageID,
    OuterPackageID,
    Brand,
    Size,
    LeadTimeDays,
    QuantityPerOuter,
    IsChillerStock,
    Barcode,
    TaxRate ,
    UnitPrice,
    RecommendedRetailPrice ,
    TypicalWeightPerUnit ,
    MarketingComments ,
    InternalComments ,
     CountryOfManufacture,
     Range ,
     Shelflife)
SELECT StockItemID,StockItemName,SupplierID,ColorID,UnitPackageID,OuterPackageID,Brand,Size,LeadTimeDays,
    QuantityPerOuter,IsChillerStock,Barcode,TaxRate,UnitPrice,RecommendedRetailPrice ,TypicalWeightPerUnit ,
    MarketingComments,InternalComments,JSON_VALUE(CustomFields,'$.CountryOfManufacture') AS CountryOfManufacture,
    JSON_VALUE(CustomFields, '$.Range') AS Range, JSON_VALUE(CustomFields,'$.Shelflife') as Shelflife
FROM Warehouse.StockItems


ALTER DATABASE SCOPED CONFIGURATION
  SET VERBOSE_TRUNCATION_WARNINGS = ON;

---23Rewrite your stored procedure in (21). Now with a given date,
---it should wipe out all the order data prior to the input date and
---load the order data that was placed in the next 7 days following the input date

DROP PROCEDURE IF EXISTS procedure1;
GO
CREATE PROCEDURE procedure1 (@date date)
AS BEGIN
BEGIN TRY

DELETE FROM Sales.Orders
WHERE OrderDate < @date

SELECT *
FROM Sales.Orders
WHERE OrderDate BETWEEN @date AND DATEADD(day, 7, @date)
END TRY
BEGIN CATCH
IF @@TRANCOUNT > 0
Rollback Transaction OrderDateTotal;
END CATCH
END;
GO

--24

select StockGroupName, [2013],[2014],[2015],[2016],[2017]
from (
select s2.StockGroupName, year(o.OrderDate) as YEARS,ol.quantity as quantity
from sales.OrderLines ol
join Warehouse.stockitems s on ol.StockItemID=s.StockItemID
join warehouse.stockitemstockgroups s1 on s.stockitemID=s1.stockitemID
join warehouse.stockgroups s2 on s1.stockgroupid=s2.stockgroupid
join Sales.Orders o on o.OrderID=ol.OrderID) as a
pivot
(
sum(quantity)
for YEARS in ([2013],[2014],[2015],[2016],[2017])
) as pivottable
FOR JSON PATH


--25

select s2.StockGroupName, year(o.OrderDate) as YEARS,SUM(ol.quantity) as quantity
from sales.OrderLines ol
join Warehouse.stockitems s on ol.StockItemID=s.StockItemID
join warehouse.stockitemstockgroups s1 on s.stockitemID=s1.stockitemID
join warehouse.stockgroups s2 on s1.stockgroupid=s2.stockgroupid
join Sales.Orders o on o.OrderID=ol.OrderID
GROUP BY s2.StockGroupName, year(o.OrderDate)
ORDER BY year(o.OrderDate)
FOR XML PATH;


---3.	List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.(question)


    SELECT DISTINCT c.CustomerName
    FROM Sales.Customers c
         JOIN
         Sales.Orders o
         ON c.CustomerID = o.CustomerID
    WHERE YEAR(o.OrderDate) < 2016
    EXCEPT
    SELECT DISTINCT c.CustomerName
    FROM Sales.Customers c
         JOIN
         Sales.Orders o
         ON c.CustomerID = o.CustomerID
    WHERE o.OrderDate > '2016-01-01'

SELECT a.CustomerName
FROM
    (
     SELECT c.CustomerName
     FROM
     Sales.Customers c
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
     WHERE YEAR(o.OrderDate)< 2016)a
WHERE a.CustomerName not in
(
     SELECT c.CustomerName
     FROM
     Sales.Customers c
     JOIN
     Sales.Orders o
     ON c.CustomerID = o.CustomerID
     WHERE o.OrderDate > '2016-01-01')
-----4.List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013’
SELECT s.StockItemName, sum(pl.OrderedOuters) as total_quantity
FROM Sales.Orders o
     JOIN
     Sales.OrderLines ol
     ON o.OrderID = ol.OrderID
     JOIN
     Warehouse.StockItems s
     ON ol.StockItemID = s.StockItemID
     JOIN
     Purchasing.PurchaseOrderLines pl
     ON s.StockItemID = pl.StockItemID
WHERE Year(o.OrderDate) = '2015'
GROUP BY s.StockItemName
----9.	List of StockItems that the company purchased more than sold in the year of 2015.

WITH a AS (
     SELECT s.StockItemName,sum(ol.Quantity) as quantity
     FROM  Sales.Orders o
     ON c.CustomerID = o.CustomerID AND YEAR(o.OrderDate) ='2015'
     JOIN
     Sales.OrderLines ol
     ON o.OrderID = ol.OrderID
     JOIN
     Warehouse.StockItems s
     ON ol.S)
SELECT s.StockItemName
FROM Purchasing.PurchaseOrders p
     JOIN
     Purchasing.PurchaseOrderLines pl
     ON p.PurchaseOrderID = pl.PurchaseOrderID
     JOIN

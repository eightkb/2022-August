 /* Row Goal*/

-- Setup environment
USE [master]
GO
ALTER DATABASE AdventureWorks2016_EXT SET COMPATIBILITY_LEVEL = 160;
GO
ALTER DATABASE AdventureWorks2016_EXT SET QUERY_STORE = ON
GO
ALTER DATABASE AdventureWorks2016_EXT SET QUERY_STORE 
	(OPERATION_MODE = READ_WRITE, 
	DATA_FLUSH_INTERVAL_SECONDS = 60, 
	INTERVAL_LENGTH_MINUTES = 1, 
	QUERY_CAPTURE_MODE = ALL)
GO
ALTER DATABASE AdventureWorks2016_EXT SET QUERY_STORE CLEAR ALL
GO
ALTER DATABASE AdventureWorks2016_EXT SET AUTOMATIC_TUNING(FORCE_LAST_GOOD_PLAN = ON)
GO

USE AdventureWorks2016_EXT
GO
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE  
GO
DBCC DROPCLEANBUFFERS
GO


-- https://aka.ms/SQLce - Cardinality Estimation docs

--Setup XE capture
IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = 'CEFeedback')
DROP EVENT SESSION [CEFeedback] ON SERVER;
GO
CREATE EVENT SESSION [CEFeedback] ON SERVER 
ADD EVENT sqlserver.query_feedback_analysis(
    ACTION(sqlserver.query_hash_signed,sqlserver.query_plan_hash_signed,sqlserver.sql_text)),
ADD EVENT sqlserver.query_feedback_validation(
    ACTION(sqlserver.query_hash_signed,sqlserver.query_plan_hash_signed,sqlserver.sql_text))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=NO_EVENT_LOSS,MAX_DISPATCH_LATENCY=1 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO

-- Start XE
ALTER EVENT SESSION [CEFeedback] ON SERVER
STATE = START;
GO

-- Run query many times (to be valid for CE Feedback)
SELECT TOP 1500 h.[SalesOrderID]
FROM Sales.SalesOrderHeader AS h 
INNER JOIN Sales.SalesOrderDetail AS d ON h.SalesOrderID = d.SalesOrderID 
WHERE (h.OrderDate >= '2014-3-28 00:00:00')
GO 15

EXEC sp_query_store_flush_db;
GO

SELECT *
FROM sys.dm_exec_cached_plans AS cp
CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS st
CROSS APPLY sys.dm_exec_query_plan_stats(plan_handle) AS qps
WHERE st.text LIKE '%FROM Sales.SalesOrderHeader AS h%';
GO

-- End of this execution should do CE feedback analysis
SELECT TOP 1500 h.[SalesOrderID]
FROM Sales.SalesOrderHeader AS h 
INNER JOIN Sales.SalesOrderDetail AS d ON h.SalesOrderID = d.SalesOrderID 
WHERE (h.OrderDate >= '2014-3-28 00:00:00')
GO 

-- This execution is used for CE feedback adjustment verification - Row Goal Analysis
SELECT TOP 1500 h.[SalesOrderID]
FROM Sales.SalesOrderHeader AS h 
INNER JOIN Sales.SalesOrderDetail AS d ON h.SalesOrderID = d.SalesOrderID 
WHERE (h.OrderDate >= '2014-3-28 00:00:00')
GO 

-- Run again, CE feedback hint will be applied 
SELECT TOP 1500 h.[SalesOrderID]
FROM Sales.SalesOrderHeader AS h 
INNER JOIN Sales.SalesOrderDetail AS d ON h.SalesOrderID = d.SalesOrderID 
WHERE (h.OrderDate >= '2014-3-28 00:00:00')
GO 15

ALTER EVENT SESSION [CEFeedback] ON SERVER
STATE = STOP;
GO

SELECT * FROM sys.query_store_query_hints
GO

SELECT * FROM sys.query_store_plan_feedback
GO





SELECT * FROM sys.query_store_plan_feedback;
GO

/*
SELECT * FROM sys.dm_exec_valid_use_hints
ORDER BY name
GO
*/

-- Reset





---------------------
-- Alternate qry 
---------------------
SELECT TOP 1500 h.SalesOrderID, h.RevisionNumber, h.OrderDate, h.DueDate, h.ShipDate, h.Status, h.OnlineOrderFlag, h.PurchaseOrderNumber, h.AccountNumber, h.CustomerID, h.SalesPersonID, h.TerritoryID, h.BillToAddressID, 
    h.ShipToAddressID, h.ShipMethodID, h.CreditCardID, h.CreditCardApprovalCode, h.CurrencyRateID, h.SubTotal, h.TaxAmt, h.Freight, h.Comment, h.rowguid, h.ModifiedDate, d.SalesOrderID AS SalesOrderID, 
	d.SalesOrderDetailID, d.CarrierTrackingNumber, d.OrderQty, d.ProductID, d.SpecialOfferID, d.UnitPrice, d.UnitPriceDiscount, d.rowguid AS rowguid, d.ModifiedDate AS ModifiedDate
FROM Sales.SalesOrderHeader AS h 
INNER JOIN Sales.SalesOrderDetail AS d ON h.SalesOrderID = d.SalesOrderID
WHERE (h.OrderDate >= '2014-4-28 00:00:00')
GO
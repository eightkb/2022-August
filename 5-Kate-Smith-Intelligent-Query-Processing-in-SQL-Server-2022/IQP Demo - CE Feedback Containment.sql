/* Containment*/

-- Setup environment
USE [master]
GO
ALTER DATABASE TPCH1G SET COMPATIBILITY_LEVEL = 160;
GO
ALTER DATABASE TPCH1G SET QUERY_STORE = ON;
GO
ALTER DATABASE TPCH1G SET QUERY_STORE 
	(OPERATION_MODE = READ_WRITE, 
	DATA_FLUSH_INTERVAL_SECONDS = 60, 
	INTERVAL_LENGTH_MINUTES = 1, 
	QUERY_CAPTURE_MODE = ALL)
GO
ALTER DATABASE TPCH1G SET QUERY_STORE CLEAR ALL
GO
ALTER DATABASE TPCH1G SET AUTOMATIC_TUNING 
	(FORCE_LAST_GOOD_PLAN = ON)
GO

USE [TPCH1G]
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
SELECT *
FROM CUSTOMER INNER JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
WHERE C_MKTSEGMENT = 'BUILDING' AND O_ORDERDATE = '1992-01-01 00:00:00.000' 
AND O_CUSTKEY < 10000
GO 15

EXEC sp_query_store_flush_db;
GO

SELECT *   
FROM sys.dm_exec_cached_plans AS cp
CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS st
CROSS APPLY sys.dm_exec_query_plan_stats(plan_handle) AS qps
WHERE st.text LIKE '%FROM CUSTOMER INNER JOIN ORDERS%';
GO

-- End of this execution should do CE feedback analysis
SELECT *
FROM CUSTOMER INNER JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
WHERE C_MKTSEGMENT = 'BUILDING' AND O_ORDERDATE = '1992-01-01 00:00:00.000' 
AND O_CUSTKEY < 10000
GO

-- There are less rows than estimated, so likely full independence is better
-- If there were more rows than estimated, then full correlation is better
-- This execution is used for CE feedback adjustment verification - Correlation Analysis
SELECT *
FROM CUSTOMER INNER JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
WHERE C_MKTSEGMENT = 'BUILDING' AND O_ORDERDATE = '1992-01-01 00:00:00.000' 
AND O_CUSTKEY < 10000
GO

-- Run again, CE feedback hint will be applied 
SELECT *
FROM CUSTOMER INNER JOIN ORDERS ON C_CUSTKEY = O_CUSTKEY
WHERE C_MKTSEGMENT = 'BUILDING' AND O_ORDERDATE = '1992-01-01 00:00:00.000' 
AND O_CUSTKEY < 10000
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

SELECT * FROM sys.dm_exec_valid_use_hints
ORDER BY name
GO

-- Reset
DBCC TRACEOFF (12612, -1)
GO
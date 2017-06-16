USE [DM_1305_GroupMBenchmarkingUS];

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Create date: 06/12/2017
-- Description:	this stored procedure launches all the other (sub) stored procedures 
-- needed to refresh the PRISMA complaince reporting tables.
--                       
-- =========================================================================

CREATE PROC [dbo].[COMPLIANCE_MAIN]
AS
-- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
SET NOCOUNT ON;

BEGIN
DECLARE @error_message NVARCHAR(4000);
DECLARE @error_severity INT ;
DECLARE @error_state INT ;

DECLARE @run_id INT;
DECLARE @log_table VARCHAR(50);
DECLARE @start_date VARCHAR(50); 
DECLARE @end_date VARCHAR(50); 

-- -- Wipe existing log table entries 
-- DELETE a
-- FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Log] a
-- WHERE a.PID <> 0

-- Instead of auto-incrementing INT, I can use something like this: --CONVERT(VARCHAR, GETDATE(), 112);
SET @run_id = (SELECT MAX(PID) FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Log])+1;
SET @log_table = 'Compliance_Log';
SET @start_date = '2014-01-01'; 
-- Adjust @end_date accordingly; Now, it's set to the end of the most recent month.
SET @end_date = CONVERT(DATE, Dateadd(s, -1, Dateadd(mm, Datediff(m, 0, Getdate()), 0))); 


BEGIN TRY

EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '1: Making a copy of BuyOrderDetails table'
EXEC [dbo].[COMPLIANCE_STEP1_COPY_BUYORDERDETAILS]

EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '2: Pivoting Advanced Placement Details table'
EXEC [dbo].[COMPLIANCE_STEP2_PIVOT_ADVANCEDPLACEMENTDETAILS]

EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '3: Joining Placements and PlacementMonthly tables'
EXEC [dbo].[COMPLIANCE_STEP3_JOIN_PLACEMENT_TABLES]

--exec [dbo].[SP_RPT_STG_PRISMA_COMPLIANCE]--00:11:24
--exec [dbo].[SP_RPT_PRISMA_COMPLIANCE]--00:06:56


-- above 4 sp's ran for 27 min on 10/30/2016
/*
select getdate(),'step5'
exec [dbo].[SP_RPT_PRISMA_OVERRIDE_BM] -- run manually after changing time stamp-- ran for 51 sec on 8/31/2016

select getdate(),'step6'
exec [dbo].[SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM] -- run manually after changing time stamp--ran for 2 min 10 sec on 8/31/2016

select getdate(),'step7'
---run this SP - this is most current 
 exec  [dbo].[SP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]--- run manually after running  SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM  ---- took 1 hr 6 mins on 10/30/2016
 */

 END TRY

BEGIN CATCH
 	set @error_message  = 'Error populating table SP_RELOAD_PRISMA_API_DATA. Original error, line [' + CONVERT(VARCHAR(5), ERROR_LINE()) + ']: ' + ERROR_MESSAGE();
    set @error_severity  = ERROR_SEVERITY();
    set @error_state  = CASE ERROR_STATE() WHEN 0 THEN 1 ELSE ERROR_STATE() END;


    RAISERROR (@error_message, @error_severity, @error_state)
 END CATCH

END









GO



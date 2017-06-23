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
-- The purpose of each of the sub stored procedures are as follows:
-- exec [dbo].[COMPLIANCE_STEP1_COPY_BUYORDERDETAILS] 
-- >This SP loads buyorders monthly snapshot data for all years.
--
--exec [dbo].[COMPLIANCE_STEP2_PIVOT_ADVANCEDPLACEMENTDETAILS]
-- >This SP loads selected custom columns (AdvancedPlacementDetails) monthly snapshot 
-- data for all years.
--
--exec [dbo].[COMPLIANCE_STEP3_JOIN_CAMPAIGN_AND_PLACEMENT_TABLES]
-- >This SP loads compliance data (Campaigns +Placements + PlacementsMonthly) tables' 
-- monthly snapshot data for all years. It separates Child and Parent Package types 
-- and does the split of parent package amount amongst its child Packages.
--
--exec [dbo].[COMPLIANCE_STEP4_JOIN_ALL_PRISMA_TABLES]
-- >This SP loads compliance data (compliance+custom columns from AdvPlacementDetails) 
-- tables' monthly snapshot data for all years. 
--
-- Note: The order in which these sub-SP execute is VERY IMPORTANT.
-- =========================================================================

ALTER PROC [dbo].[COMPLIANCE_MAIN] AS
-- SET NOCOUNT ON to prevent extra result sets from interfering with the SELECT statements.
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

		EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '3: Joining Campaigns, Placements and PlacementMonthly tables'
		EXEC [dbo].[COMPLIANCE_STEP3_JOIN_CAMPAIGN_AND_PLACEMENT_TABLES]

		EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '4: Joining output table from STEP3 with the AdvancedPlacementDetails table'
		EXEC [dbo].[COMPLIANCE_STEP4_JOIN_ALL_PRISMA_TABLES]

		EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '5: Appending BuyOrderDetails data within specific date range into the final table'
		EXEC [dbo].[COMPLIANCE_STEP5_APPEND_BUYORDERDETAILS_DATA_TO_FINAL_TABLE]

		EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '6: Appending Campaign and Placement related data within specific date range into the final table'
		EXEC [dbo].[COMPLIANCE_STEP6_APPEND_PLACEMENTS_DATA_TO_FINAL_TABLE]

		EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '7: Applying business rules to create final report table'
		EXEC [dbo].[COMPLIANCE_STEP7_APPLY_BUSINESS_RULES_TO_CREATE_FINAL_REPORTING_TABLE]

		EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '8: Compliance Report ALL Stored Procedures Finished Running.'
	END TRY

	BEGIN CATCH
		set @error_message  = 'Error populating table SP_RELOAD_PRISMA_API_DATA. Original error, line [' + CONVERT(VARCHAR(5), ERROR_LINE()) + ']: ' + ERROR_MESSAGE();
		set @error_severity  = ERROR_SEVERITY();
		set @error_state  = CASE ERROR_STATE() WHEN 0 THEN 1 ELSE ERROR_STATE() END;

		RAISERROR (@error_message, @error_severity, @error_state)
	END CATCH
END

GO

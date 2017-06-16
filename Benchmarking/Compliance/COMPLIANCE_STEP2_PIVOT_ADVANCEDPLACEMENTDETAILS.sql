USE [DM_1305_GroupMBenchmarkingUS];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Create date: 06/13/2017
-- Description:	This stored procedure pivots desired (18) columns from 
-- Advanced Placement Details.
--                       
-- =========================================================================
CREATE PROC [dbo].[COMPLIANCE_STEP2_PIVOT_ADVANCEDPLACEMENTDETAILS] AS

BEGIN
IF OBJECT_ID('Compliance_AdvPlacementDetails') IS NOT NULL
   DROP TABLE Compliance_AdvPlacementDetails

	SELECT *
	INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails]
	FROM 
	( 
		SELECT [PlacementId], [CustomColumnName], [CustomColumnValue] 
		FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054940_Prisma_Advanced_Placement_Details_Extracted]
	) AS SourceTable
	PIVOT
	(
		MAX([CustomColumnValue])
		FOR [CustomColumnName] IN (
				[AD FORMAT FOR NON-1X1 UNITS], 
				[BUY TYPE], 
				[BUY TYPE 2], 
				[Channel Type 1], 
				[Channel Type 2], 
				[CONTENT CHANNEL DETAILS],
				[CPA KPI],
				[PRIMARY CONTENT CHANNEL],
				[RICH MEDIA TYPES - FORMAT],
				[RICH MEDIA/4TH PARTY VENDOR],
				[SECONDARY CONTENT CHANNEL],
				[Targeting Audience Type],
				[Targeting Context Type],
				[Targeting Delivery Type],
				[TARGETING TYPE 1],
				[TARGETING TYPE 2],
				[TRACKING METHOD]
		)
	) AS PivotTable;
		
END
GO

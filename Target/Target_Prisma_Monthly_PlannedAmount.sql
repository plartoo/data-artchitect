USE [DM_1304_Target];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Create date: 06/26/2017
-- Description:	This stored procedure joins Placements, Placement Monthly and
-- Adv. Placement Detail tables to generate a report for client 'Target' that 
-- shows monthly planned spend in Prisma at the placement level, including 
-- the followings:
--
-- Prisma Campaign
-- Prisma Placement Name
-- Month
-- Prisma Planned Spend
-- "Targeting Delivery Type" attribute from Adv Placements Details
-- "GroupM Channel" attribute from Adv Placements Details
-- =========================================================================

ALTER PROC [dbo].[AdHoc_Katelin_Prisma_Monthly_PlannedAmount] AS 
BEGIN

	-- Step1: make a copy of Adv Placement Details with columns of interest
	IF OBJECT_ID('Tmp_Katelin_Prisma_Monthly_PlannedAmount_AdvPlacementDetails') IS NOT NULL
	   DROP TABLE Tmp_Katelin_Prisma_Monthly_PlannedAmount_AdvPlacementDetails

	SELECT *
	INTO [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_AdvPlacementDetails]
	FROM 
	( 
	SELECT [PlacementId], [CustomColumnName], [CustomColumnValue] 
	FROM [DM_1304_Target].[dbo].[DFID041107_Prisma_AdvancedPlacementDetails_GT_Target_US_Extracted]
	) AS SourceTable
	PIVOT
	(
	MAX([CustomColumnValue])
	FOR [CustomColumnName] IN (
			[GROUPM CHANNEL],
			[Targeting Delivery Type]
		)
	) AS PivotTable;

	-- Step2: make a copy of Placement Monthly 
	IF OBJECT_ID('Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly') IS NOT NULL
	   DROP TABLE Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly

	SELECT
	--[AdvertiserCode]
	--,[AdvertiserName]
	[CampaignId]
	,[CampaignName]
	,[CampaignStartDate]
	,[CampaignEndDate]
	,[PlacementId]
	,[PlacementName]
	,[PlacementStartDate]
	,[PlacementEndDate]
	--,[PlacementMonthlyStartDate]
	--,[PlacementMonthlyEndDate]
	,CASE WHEN (([PlacementMonth] IS NULL) OR ([PlacementYear] IS NULL)) THEN NULL
	ELSE CONVERT(SMALLDATETIME, CONCAT(CONVERT(VARCHAR, [PlacementMonth]), '-01-', CONVERT(VARCHAR, [PlacementYear])))
	END AS [PlacementMonth]

	,[PackageId]
	,[PackageType]
	,[ParentId]

	,[SupplierCode]
	,[SupplierName]
	,[BuyType]

	,[PlannedAmount] --AS [Original_PlannedAmount]
	,[PlannedClicks] --AS [Original_PlannedClicks]
	,[PlannedImpressions] --AS [Original_PLannedImpressions]
	,[PlannedUnits] --AS [Original_PlannedUnits]
	,[PlannedActions] --AS [Original_PlannedActions]
	--,CAST(NULL AS float) AS [Weighted_PlannedAmount]
	--,CAST(NULL AS float) AS [Weighted_PlannedClicks]
	--,CAST(NULL AS float) AS [Weighted_PlannedImpressions]
	--,CAST(NULL AS float) AS [Weighted_PlannedUnits]
	--,CAST(NULL AS float) AS [Weighted_PlannedActions]
	--,SUM(1) OVER (PARTITION BY [ParentId], [PackageType], [PlacementMonthlyStartDate], [PlacementMonthlyEndDate]) as [Child_Count]
	--,[SupplierActions]
	--,[SupplierClicks]
	--,[SupplierCost]
	--,[SupplierImpressions]
	--,[SupplierUnits]
	--,CASE WHEN ([PackageType] IN ('Package')) THEN 'P' ELSE 'C' END AS [RecordType]
	INTO [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly]
	FROM [DM_1304_Target].[dbo].[DFID041103_Prisma_MonthlyDelivery_GT_Target_US_Extracted]

/*
-- Don't do weighing yet, as per Shu's request.
	UPDATE A
	SET			
				A.[Weighted_PlannedAmount] = (1.0 * B.[Original_PlannedAmount])/A.[Child_Count]
				,A.[Weighted_PlannedClicks] = (1.0 * B.[Original_PlannedClicks])/A.[Child_Count]
				,A.[Weighted_PlannedImpressions] = (1.0 * B.[Original_Impressions])/A.[Child_Count]
				,A.[Weighted_PlannedUnits] = (1.0 * B.[Original_PlannedUnits])/A.[Child_Count]
				,A.[Weighted_PlannedActions] = (1.0 * B.[Original_PlannedActions])/A.[Child_Count]
	FROM		(SELECT * FROM [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly] WHERE [PackageType] = 'Child') AS A
	INNER JOIN	(SELECT * FROM [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly] AS C WHERE C.[PackageType] = 'Package') AS B
	ON A.[ParentId] = B.[PlacementId] -- B.[ParentId] is okay too
	AND A.[PlacementMonth] = B.[PlacementMonth]
*/
	-- Step 3: Combine PlacementMonthly and Adv Placement Details into one final reporting table.
	IF OBJECT_ID('Katelin_Prisma_Monthly_PlannedAmount_Final') IS NOT NULL
	   DROP TABLE Katelin_Prisma_Monthly_PlannedAmount_Final

	SELECT A.*, B.[GROUPM CHANNEL], B.[TARGETING DELIVERY TYPE]
	INTO [DM_1304_Target].[dbo].[Katelin_Prisma_Monthly_PlannedAmount_Final]
	FROM [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly] AS A
	INNER JOIN [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_AdvPlacementDetails] AS B
	ON A.[PlacementId] = B.[PlacementId]

	DROP TABLE [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_AdvPlacementDetails];
	DROP TABLE [DM_1304_Target].[dbo].[Tmp_Katelin_Prisma_Monthly_PlannedAmount_PlacementMonthly];

END
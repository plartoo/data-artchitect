USE [DM_1305_GroupMBenchmarkingUS];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[COMPLIANCE_STEP7_APPLY_BUSINESS_RULES_TO_CREATE_FINAL_REPORTING_TABLE]
AS

-- =========================================================================
-- Author:		Phyo Thiha
-- Modified Date: 06/21/2017
-- Description:	This stored procedure applies business rules to raw tables and
-- creates the final Compliance reporting table out of that.
-- =========================================================================

DECLARE @refreshed_date DATE
SET @refreshed_date = CONVERT(DATE, DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, GETDATE()), 0))) --the last date of the most recent month 

IF OBJECT_ID('Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined') IS NULL
	CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
	(
		 [AgencyName] NVARCHAR(4000)
		,[AgencyAlphaCode] NVARCHAR(4000)
		,[LocationCompanyCode] NVARCHAR(4000)
		,[CampaignStartDate] SMALLDATETIME
		,[CampaignEndDate] SMALLDATETIME
		,[CampaignStatus] NVARCHAR(4000)
		,[RateType] NVARCHAR(4000)
		,[Budget] FLOAT
		,[BudgetApproved] NVARCHAR(4000)
		,[CampaignUser] NVARCHAR(4000)
		
		,[MediaCode] NVARCHAR(4000)
		,[MediaName] NVARCHAR(4000)
		,[AdvertiserCode] NVARCHAR(4000)
		,[AdvertiserName] NVARCHAR(4000)
		,[ProductCode] NVARCHAR(4000)
		,[ProductName] NVARCHAR(4000)
		,[EstimateCode] NVARCHAR(4000)
		,[EstimateName] NVARCHAR(4000)
		,[EstimateStartDate] SMALLDATETIME
		,[EstimateEndDate] SMALLDATETIME
		,[SupplierCode] NVARCHAR(4000)
		,[SupplierName] NVARCHAR(4000)
		,[BuyType] NVARCHAR(4000)
		,[BuyCategory] NVARCHAR(4000)
		,[CampaignId] INT
		,[CampaignPublicId] NVARCHAR(4000)
		,[CampaignName] NVARCHAR(4000)
		,[PackageType] NVARCHAR(4000)
		,[PackageId] INT
		,[PlacementId] INT
		,[ParentId] INT
		,[PlacementName] NVARCHAR(4000)
		,[PlacementType] NVARCHAR(4000)
		,[Site] NVARCHAR(4000)
		,[Dimension] NVARCHAR(4000)
		,[Positioning] NVARCHAR(4000)
		,[CostMethod] NVARCHAR(4000)
		,[UnitType] NVARCHAR(4000)
		,[Rate] FLOAT
		,[IONumber] NVARCHAR(4000)
		,[ServedBy] NVARCHAR(4000)
		,[AdserverName] NVARCHAR(4000)
		,[AdserverSupplierName] NVARCHAR(4000)

		,[New_PlacementMonth] SMALLDATETIME
		,[PlacementMonth] INT
		,[PlacementYear] INT
		,[PlacementMonthlyStartDate] SMALLDATETIME
		,[PlacementMonthlyEndDate] SMALLDATETIME
		,[PlannedAmount] FLOAT 
		,[PlannedUnits] BIGINT
		,[PlannedImpressions] BIGINT
		,[PlannedClicks] BIGINT
		,[PlannedActions] BIGINT
		,[IOAmount] FLOAT
		,[SupplierUnits] BIGINT	
		,[SupplierImpressions] BIGINT
		,[SupplierClicks] BIGINT
		,[SupplierActions] BIGINT
		,[SupplierCost] FLOAT
		,[AdserverUnits] BIGINT	
		,[AdserverImpressions] BIGINT
		,[AdserverClicks] BIGINT
		,[AdserverActions] BIGINT
		,[AdserverCost] FLOAT
		,[DeliveryExists] NVARCHAR(4000)
		,[MasterClientName] NVARCHAR(4000)
		,[ChildCount] INT

		,[AD FORMAT FOR NON-1X1 UNITS] NVARCHAR(4000)
		,[Channel Type 1] NVARCHAR(4000)
		,[BUY TYPE 2] NVARCHAR(4000)
		,[Channel Type 2] NVARCHAR(4000)
		,[Rich Media Types - Format] NVARCHAR(4000)
		,[RICH MEDIA/4TH PARTY VENDOR] NVARCHAR(4000)
		,[TARGETING TYPE 2] NVARCHAR(4000)
		,[BUY TYPE] NVARCHAR(4000)
		,[TARGETING TYPE 1] NVARCHAR(4000)
		,[Targeting Audience Type] NVARCHAR(4000)
		,[Targeting Delivery Type] NVARCHAR(4000)
		,[CPA KPI] NVARCHAR(4000)
		,[SECONDARY CONTENT CHANNEL] NVARCHAR(4000)
		,[TRACKING METHOD] NVARCHAR(4000)
		,[Targeting Context Type] NVARCHAR(4000)
		,[PRIMARY CONTENT CHANNEL] NVARCHAR(4000)
		,[CONTENT CHANNEL DETAILS] NVARCHAR(4000)
		,[Refreshed_Date] SMALLDATETIME

		,[1By1Acceptable] INT
		,[1X1Exists] INT
		,[COUNTRY] NVARCHAR(10)
		,[DataDeliveryExists] INT
		,[PlannedDataExists] INT
		,[Record] NVARCHAR(100)
		,[NA Column Count] INT
		,[1By1IneligiblePlanned Amount] FLOAT
		,[1By1IneligiblePlannedImpressions] BIGINT
		,[NA Placements] INT
		,[Placements without Delivery] INT
		,[Invalid 1x1 Placements] INT
		,[Invalid1by1PlannedCost] FLOAT
		,[PlannedCostWithoutDelivery] FLOAT
		,[NAPlannedCost] FLOAT
	)

INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]	( -- in Pavani's code, this table was: [ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
	-- columns from Campaigns table
	[AgencyName],[AgencyAlphaCode],[LocationCompanyCode],[CampaignStartDate],[CampaignEndDate],[CampaignStatus],[RateType],[Budget],[BudgetApproved],[CampaignUser],

	-- columns from Placements (Details) table
	[MediaCode],[MediaName],[AdvertiserCode],[AdvertiserName],[ProductCode],[ProductName],[EstimateCode],[EstimateName],[EstimateStartDate],
	[EstimateEndDate],[SupplierCode],[SupplierName],[BuyType],[BuyCategory],[CampaignId],[CampaignPublicId],[CampaignName],[PackageType],[PackageId],
	[PlacementId],[ParentId],[PlacementName],[PlacementType],[Site],[Dimension],[Positioning],[CostMethod],[UnitType],[Rate],[IONumber],
	[ServedBy], [AdserverName], [AdserverSupplierName], --[PlacementCategory],
	
	-- columns from Placements Monthly table
	[New_PlacementMonth],[PlacementMonth],[PlacementYear],[PlacementMonthlyStartDate],[PlacementMonthlyEndDate],[PlannedAmount],[PlannedUnits],[PlannedImpressions],[PlannedClicks],[PlannedActions],
	[IOAmount],[SupplierUnits],[SupplierImpressions],[SupplierClicks],[SupplierActions],[SupplierCost],[AdserverUnits],[AdserverImpressions],[AdserverClicks],
	[AdserverActions],[AdserverCost],[DeliveryExists],[MasterClientName],[ChildCount],
	
	-- columns from Advanced Placements table
	[AD FORMAT FOR NON-1X1 UNITS],[Channel Type 1],[BUY TYPE 2],
	[Channel Type 2],[Rich Media Types - Format],[RICH MEDIA/4TH PARTY VENDOR],
	[TARGETING TYPE 2],[BUY TYPE],[TARGETING TYPE 1],[Targeting Audience Type],
	[Targeting Delivery Type],[CPA KPI],[SECONDARY CONTENT CHANNEL],[TRACKING METHOD],
	[Targeting Context Type],[PRIMARY CONTENT CHANNEL],[CONTENT CHANNEL DETAILS],
	[Refreshed_Date],

	-- new columns created based on business rules
	[1By1Acceptable],[1X1Exists],[COUNTRY],[DataDeliveryExists],
	[PlannedDataExists],[Record],
	[NA Column Count])
SELECT 
	[AgencyName],[AgencyAlphaCode],[LocationCompanyCode],[CampaignStartDate],[CampaignEndDate],[CampaignStatus],[RateType],[Budget],[BudgetApproved],[CampaignUser], 
	
	[MediaCode],[MediaName],[AdvertiserCode],[AdvertiserName],[ProductCode],[ProductName],[EstimateCode],[EstimateName],[EstimateStartDate],
	[EstimateEndDate],[SupplierCode],[SupplierName],[BuyType],[BuyCategory],[CampaignId],[CampaignPublicId],[CampaignName],[PackageType],[PackageId],
	[PlacementId],[ParentId],[PlacementName],[PlacementType],[Site],[Dimension],[Positioning],[CostMethod],[UnitType],[Rate],[IONumber],
	[ServedBy], [AdserverName], [AdserverSupplierName],
	
	[New_PlacementMonth],[PlacementMonth],[PlacementYear],[PlacementMonthlyStartDate],[PlacementMonthlyEndDate],[PlannedAmount],[PlannedUnits],[PlannedImpressions],[PlannedClicks],[PlannedActions],
	[IOAmount],[SupplierUnits],[SupplierImpressions],[SupplierClicks],[SupplierActions],[SupplierCost],[AdserverUnits],[AdserverImpressions],[AdserverClicks],
	[AdserverActions],[AdserverCost],[DeliveryExists],[MasterClientName],[ChildCount],
	
	[AD FORMAT FOR NON-1X1 UNITS],[Channel Type 1],[BUY TYPE 2],
	[Channel Type 2],[Rich Media Types - Format],[RICH MEDIA/4TH PARTY VENDOR],
	[TARGETING TYPE 2],[BUY TYPE],[TARGETING TYPE 1],[Targeting Audience Type],
	[Targeting Delivery Type],[CPA KPI],[SECONDARY CONTENT CHANNEL],[TRACKING METHOD],
	[Targeting Context Type],[PRIMARY CONTENT CHANNEL],[CONTENT CHANNEL DETAILS],
	[Refreshed_Date],

	CASE 
	WHEN
		(
		(LOWER([Positioning]) LIKE '%email%'
		OR LOWER([Positioning]) LIKE '%social%'
		OR LOWER([Positioning]) LIKE '%value%'
		OR LOWER([Positioning]) LIKE '%editorial%'
		OR LOWER([Positioning]) LIKE '%logo%'
		OR LOWER([Positioning]) LIKE '%text%'
		OR LOWER([Positioning]) LIKE '%key word%'
		OR LOWER([Positioning]) LIKE '%keyword%'
		OR LOWER([Positioning]) LIKE '%billboard%'
		OR LOWER([Positioning]) LIKE '%tracking pixel%'
		OR LOWER([Positioning]) LIKE '%accounts%'
		OR LOWER([Positioning]) LIKE '%advertorial%'
		OR LOWER([Positioning]) LIKE '%articles%'
		OR LOWER([Positioning]) LIKE '%audio%'
		OR LOWER([Positioning]) LIKE '%backdrop%'
		OR LOWER([Positioning]) LIKE '%background%'
		OR LOWER([Positioning]) LIKE '%blast%'
		OR LOWER([Positioning]) LIKE '%blog%'
		OR LOWER([Positioning]) LIKE '%blogger%'
		OR LOWER([Positioning]) LIKE '%blogvertorial unit%'
		OR LOWER([Positioning]) LIKE '%canceled%'
		OR LOWER([Positioning]) LIKE '%cancelled%'
		OR LOWER([Positioning]) LIKE '%characters%'
		OR LOWER([Positioning]) LIKE '%co-brand%'
		OR LOWER([Positioning]) LIKE '%eblast%'
		OR LOWER([Positioning]) LIKE '%ebook%'
		OR LOWER([Positioning]) LIKE '%facebook%'
		OR LOWER([Positioning]) LIKE '%fb%'
		OR LOWER([Positioning]) LIKE '%fbx%'
		OR LOWER([Positioning]) LIKE '%fee%'
		OR LOWER([Positioning]) LIKE '%foursquare%'
		OR LOWER([Positioning]) LIKE '%header%'
		OR LOWER([Positioning]) LIKE '%instagram%'
		OR LOWER([Positioning]) LIKE '%link%'
		OR LOWER([Positioning]) LIKE '%logo%'
		OR LOWER([Positioning]) LIKE '%pinterest%'
		OR LOWER([Positioning]) LIKE '%post%'
		OR LOWER([Positioning]) LIKE '%print%'
		OR LOWER([Positioning]) LIKE '%seeding%'
		OR LOWER([Positioning]) LIKE '%skin%'
		OR LOWER([Positioning]) LIKE '%sleeve%'
		OR LOWER([Positioning]) LIKE '%static wrap%'
		OR LOWER([Positioning]) LIKE '%study%'
		OR LOWER([Positioning]) LIKE '%survey%'
		OR LOWER([Positioning]) LIKE '%tout%'
		OR LOWER([Positioning]) LIKE '%tumblr%'
		OR LOWER([Positioning]) LIKE '%tweet%'
		OR LOWER([Positioning]) LIKE '%twitter%'
		OR LOWER([Positioning]) LIKE '%wallpaper%')
		AND (LOWER([Positioning]) <> 'contextual')
		AND (LOWER([Positioning]) <> 'contextual static/flash')) 
		OR ([ServedBy]='3rd party' AND [Dimension]='1 x 1')
		OR ([Dimension]='' AND 
			([PackageType]='FeeOrder' 
			OR [PackageType]='Package' 
			OR [PackageType]='Roadblock' 
			OR [PackageType]='SearchOrder'))
	THEN 1
	ELSE 0
	END AS [1By1Acceptable],
	
	CASE 
	WHEN 
		[Dimension] IS NULL
		OR [Dimension]= '1 x 1' 
		OR LOWER([Dimension]) = 'n/a' 
		OR [Dimension]= '' 
	THEN 1
	ELSE 0
	END AS [1X1Exists],
	
	CASE 
	WHEN 
		[AgencyAlphaCode]='H7' 
	THEN 'US' 
	ELSE 'Canada' 
	END AS [COUNTRY],
	
	CASE
	WHEN  
		CASE WHEN [CostMethod]='Flat' 
			THEN [PlannedAmount] 
			ELSE [AdserverCost] 
		END > 0
		AND 
			([AdserverActions] > 0 
				OR [AdserverClicks] > 0 
				OR [AdserverImpressions] > 0 
				OR [AdserverUnits] > 0) 
			OR 
			CASE WHEN [CostMethod]='Flat' 
				THEN [PlannedAmount] 
				ELSE [SupplierCost] END > 0 
		AND (
			[SupplierUnits] > 0
			OR [SupplierActions] > 0
			OR [SupplierClicks] > 0 
			OR [SupplierImpressions]>0
			)
		OR 
		CASE 
		WHEN 
		([CostMethod]='Free' AND [PlannedAmount] = 0) THEN [PlannedAmount] 
		ELSE [PlannedAmount] END = 0
		AND
		   (
		   [AdserverActions] > 0 
		   OR [AdserverClicks] > 0 
		   OR [AdserverImpressions] > 0 
		   OR [AdserverUnits] > 0
		   OR [SupplierUnits]>0 
		   OR [SupplierActions]>0 
		   OR [SupplierClicks]>0 
		   OR [SupplierImpressions]>0
		   )
	THEN 1
	ELSE 0
	END AS [DataDeliveryExists],
	
	CASE
	WHEN
		[PlannedAmount] = 0 AND [PlannedActions] = 0 AND [PlannedClicks] = 0 AND
		[PlannedImpressions] = 0 AND [PlannedUnits]= 0
	THEN 0
	ELSE 1
	END AS [PlannedDataExists],
	
	CONVERT(NVARCHAR(30),[PlacementId]) + ' ' + [PlacementMonth] + ' ' + [PlacementYear] AS [Record],
	
	CASE WHEN UPPER([AD FORMAT FOR NON-1X1 UNITS])='N/A' OR UPPER([AD FORMAT FOR NON-1X1 UNITS])='NA' THEN 1 ELSE 0 END
	+ CASE WHEN UPPER([BUY TYPE])='N/A' OR UPPER([BUY TYPE])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN UPPER([BUY TYPE 2])='N/A' OR UPPER([BUY TYPE 2])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN upper([Channel Type 1])='N/A' or upper([Channel Type 1])='NA' then 1 else 0 end
	+ CASE WHEN upper([Channel Type 2])='N/A' or upper([Channel Type 2])='NA'then 1 else 0 end
	+ CASE WHEN UPPER([CONTENT CHANNEL DETAILS])='N/A' OR UPPER([CONTENT CHANNEL DETAILS])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN UPPER([CPA KPI])='N/A' OR UPPER([CPA KPI])='NA'THEN '1' ELSE 0 END
	+ CASE WHEN upper([Positioning])='N/A' or upper([Positioning])='NA'then 1 else 0 end
	+ CASE WHEN UPPER([PRIMARY CONTENT CHANNEL])='N/A' OR UPPER([PRIMARY CONTENT CHANNEL])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN upper([Rich Media Types - Format])='N/A' or upper([Rich Media Types - Format])='NA'then 1 else 0 end
	+ CASE WHEN UPPER([RICH MEDIA/4TH PARTY VENDOR])='N/A' OR UPPER([RICH MEDIA/4TH PARTY VENDOR])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN UPPER([SECONDARY CONTENT CHANNEL])='N/A' OR UPPER([SECONDARY CONTENT CHANNEL])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN upper([Targeting Audience Type])='N/A' or upper([Targeting Audience Type])='NA'then 1 else 0 end
	+ CASE WHEN upper([Targeting Context Type])='N/A' or upper([Targeting Context Type])='NA'or upper([Targeting Context Type])='NA' then 1 else 0 end
	+ CASE WHEN upper([Targeting Delivery Type])='N/A' or upper([Targeting Delivery Type])='NA'then 1 else 0 end
	+ CASE WHEN UPPER([TARGETING TYPE 1])='N/A' OR UPPER([TARGETING TYPE 1])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN UPPER([TARGETING TYPE 2])='N/A' OR UPPER([TARGETING TYPE 2])='NA'THEN 1 ELSE 0 END
	+ CASE WHEN UPPER([TRACKING METHOD])='N/A' OR UPPER([TRACKING METHOD])='NA'THEN 1 ELSE 0 END as [NA Column Count]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly_Final]
WHERE  [Refreshed_Date] = @refreshed_date

--delete records with CampaignStatus = 'Deleted' (deleted campaigns) 
DELETE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
WHERE  CampaignStatus = 'Deleted'
AND [Refreshed_Date] = @refreshed_date

/*
-- Unlike Pavani's code, we don't need to worry about deleting stuff which we never have
DELETE [dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
Where Refreshed_Date = '2015-03-30 00:00:00.000' --will be no need in the statement
DELETE [dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
Where Refreshed_Date = '2015-08-30 00:00:00.000' --will be no need in the statement
DELETE [dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
Where Refreshed_Date = '2015-09-22 00:00:00.000' --will be no need in the statement
*/

--delete records with AgencyName='GARAGE TEAM MAZDA'
DELETE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
WHERE UPPER([AgencyName]) = 'GARAGE TEAM MAZDA'
AND [Refreshed_Date] = @refreshed_date

--update [1By1IneligiblePlanned Amount], [1By1IneligiblePlannedImpressions], [NA Placements], [Placements without Delivery], [Invalid 1x1 Placements], [Invalid1by1PlannedCost], [PlannedCostWithoutDelivery],
--[NAPlannedCost] columns using logic specified for reporting purposes by Todd Snyder and Jim Mulvey
UPDATE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
SET 
	[1By1IneligiblePlanned Amount] = 
		CASE WHEN [1By1Acceptable] = 0 then [PlannedAmount]
		ELSE NULL END,
	[1By1IneligiblePlannedImpressions] = 
		CASE WHEN [1By1Acceptable] = 0 then [PlannedImpressions]
		ELSE NULL END,
	[NA Placements] = 
		CASE WHEN [NA Column Count] >= 4 THEN 1 ELSE 0 END,
	[Placements without Delivery] = 
		CASE WHEN [DataDeliveryExists] = 0 then 1  ELSE 0 END,
	[Invalid 1x1 Placements] = 
		CASE WHEN [1By1Acceptable] = 0 and [1X1Exists] = 1 then 1 ELSE 0 END,
	[Invalid1by1PlannedCost] = 
		CASE  WHEN [1By1Acceptable] = 0 and [1X1Exists] = 1 then [PlannedAmount] ELSE 0 END,
	[PlannedCostWithoutDelivery] = 
		CASE WHEN [DataDeliveryExists] = 0  then [PlannedAmount] ELSE 0 END,
	[NAPlannedCost] = 
		CASE WHEN [NA Column Count] >= 4 THEN [PlannedAmount] ELSE 0 END
WHERE [Refreshed_Date] = @refreshed_date

-----------------------------------------------------
-- Create Summary table out of the BuyOrderDetails table.
-- Here, we insert only records with [BuyType] in ('Display','Search'), and
-- exclude records with AgencyName='GARAGE TEAM MAZDA'.
IF OBJECT_ID('Compliance_Report_Final_BOD_And_Placements_Combined') IS NULL
	CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
	(
		[Country] NVARCHAR(10)
		,[AgencyName] NVARCHAR(10)
		,[MasterClientName] NVARCHAR(10)
		,[AdvertiserName] NVARCHAR(4000)
		,[AdvertiserCode] NVARCHAR(4000)
		,[CampaignUser] NVARCHAR(4000)
		,[CampaignPublicId] NVARCHAR(4000)
		,[CampaignName] NVARCHAR(4000)
		,[RefreshedDate] SMALLDATETIME
		,[Month] SMALLDATETIME
		,[# of Records] BIGINT
		,[# of Buys Overridden] BIGINT
		,[BuyAmount] FLOAT
		,[OverrideDollars] FLOAT
		,[RecordType] NVARCHAR(10)

		,[PlacementId]
		,[CostMethod]
		,[# of Placements]
		,[Invalid 1X1 Placements]
		,[Placements without Delivery]
		,[NA Placements]
		,[Invalid1by1PlannedCost]
		,[PlannedCostWithoutDelivery]
		,[NAPlannedCost]
		,[PlannedAmount]
	)

INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined](
	[Country]
	,[AgencyName]
	,[MasterClientName]
	,[AdvertiserName]
	,[AdvertiserCode]
	,[CampaignUser]
	,[CampaignPublicId]
	,[CampaignName]
	,[RefreshedDate]
	,[New_BuyMonth]
	,[Month], 
	,[# of Records]
	,[# of Buys Overridden]
	,[BuyAmount]
	,[OverrideDollars]
	,[RecordType]
	)
SELECT 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END as [Country]
	,[AgencyName]
	,UPPER(ISNULL([MasterClientName],'')) AS [MasterClientName]
	,[AdvertiserName]
	,[AdvertiserCode]
	,[CampaignUser]
	,[CampaignPublicId]
	,[CampaignName]
	,[RefreshedDate]
	,[New_BuyMonth]
	,COUNT(*) AS [# of Records]
	,SUM(CONVERT(BIGINT,[IsOverride])) AS [# of Buys Overridden]
	,SUM([BuyAmount]) AS [BuyAmount]
	,CASE WHEN SUM(CONVERT(INT,[IsOverride])) >= 1 THEN SUM([BuyAmount]) ELSE 0 END AS [OverrideDollars]
	,'O' as [RecordType]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails_Final]
WHERE 
	[BuyType] in ('Display','Search') 
	AND UPPER(AgencyName) <> 'GARAGE TEAM MAZDA'
	AND [Refreshed_Date] = @refreshed_date
GROUP BY 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END
	,[AgencyName]
	,ISNULL([MasterClientName],'')
	,[AdvertiserName]
	,[AdvertiserCode]
	,[CampaignUser]
	,[CampaignPublicId]
	,[CampaignName]
	,[RefreshedDate]
	,[New_BuyMonth]
ORDER BY -- Note by Phyo: I don't know why they order records here; very inefficient
	[Country]
	,[AgencyName]
	,ISNULL([MasterClientName],'')
	,[AdvertiserName]
	,[AdvertiserCode]

/*
-- Unlike Pavani's code, we don't need to worry about deleting stuff which we never have
DELETE [dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
WHERE Refreshed_Date = '2015-03-30 00:00:00.000'--will be no need in the statement
DELETE [dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
Where Refreshed_Date = '2015-08-30 00:00:00.000'--will be no need in the statement
DELETE [dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
Where Refreshed_Date = '2015-09-22 00:00:00.000'--will be no need in the statement
*/

--create temp table to calculate [OverrideDollars] amount
IF OBJECT_ID('Compliance_Tmp_Report_Full_From_BuyOrderDetails_Table') IS NOT NULL
	DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Report_Full_From_BuyOrderDetails_Table]

SELECT 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END AS [Country]
	,[AgencyName]
	,UPPER(ISNULL([MasterClientName],'')) AS [MasterClientName]
	,[AdvertiserName]
	,[AdvertiserCode]
	,[CampaignUser]
	,[CampaignPublicId]
	,[CampaignName]
	,[RefreshedDate]
	,[New_BuyMonth]
	,[BuyMonth]
	,SUM([BuyAmount]) AS [OverrideDollars]
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Report_Full_From_BuyOrderDetails_Table]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails_Final] AS s
WHERE 
	[BuyType] IN ('Display','Search')
	AND UPPER(s.[AgencyName]) <> 'GARAGE TEAM MAZDA' 
	AND s.[Isoverride] = 1
	AND s.[Refreshed_Date] = @refreshed_date
GROUP BY 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END
	,[AgencyName]
	,ISNULL([MasterClientName],'')
	,[AdvertiserName]
	,[AdvertiserCode]
	,[CampaignUser]
	,[CampaignPublicId]
	,[CampaignName]
	,[RefreshedDate]
	,[New_BuyMonth]
	,[BuyMonth]

--update [OverrideDollars] column in [Compliance_Report_Final_BOD_And_Placements_Combined] table with values from [OverrideDollars] column in temp table
UPDATE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined] b
SET [OverrideDollars] = a.[OverrideDollars]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Report_Full_From_BuyOrderDetails_Table] a
WHERE 
	b.[Country] = a.[Country]
	AND b.[AgencyName] = a.[AgencyName]
	AND b.[MasterClientName] = a.[MasterClientName]
	AND b.[AdvertiserName] = a.[AdvertiserName]
	AND b.[AdvertiserCode] = a.[AdvertiserCode]
	AND b.[CampaignUser] = a.[CampaignUser]
	AND b.[CampaignPublicId] = a.[CampaignPublicId]
	AND b.[CampaignName] = a.[CampaignName]
	AND b.[Refreshed_Date] = a.[Refreshed_Date]
	AND b.[New_BuyMonth] = a.[New_BuyMonth]
	AND b.[BuyMonth] = a.[BuyMonth]
	AND b.[RecordType] = 'O'

DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Report_Full_From_BuyOrderDetails_Table];
























































--::: I am here
-- Insert records from [Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined] table 
-- into [Compliance_Report_Final_BOD_And_Placements_Combined] table.
-- Applying filters [BuyType] in ('Display','Search') and 
-- PackageType in ('Child','Standalone') and [PlannedDataExists] = 1 (business rules provided by Todd Snyder)
INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
	(
		[Country]
		,[AgencyName]
		,[MasterClientName]
		,[AdvertiserName]
		,[AdvertiserCode]
		,[CampaignUser]
		,[CampaignPublicId]
		,[CampaignName]
		,[RefreshedDate]
		,[Month] -- here, this is actually the PlacementMonth from Placements tables
		,[PlacementId]
		,[CostMethod]
		,[# of Placements]
		,[Invalid 1X1 Placements]
		,[Placements without Delivery]
		,[NA Placements]
		,[Invalid1by1PlannedCost]
		,[PlannedCostWithoutDelivery]
		,[NAPlannedCost]
		,[PlannedAmount]
		,[# of Records]
		,[RecordType]
	)
SELECT 
	[Country]
	,[AgencyName]
	,UPPER([MasterClientName])
	,[AdvertiserName]
	,[AdvertiserCode]
	,[CampaignUser]
	,[CampaignPublicId]
	,[CampaignName]
	,[RefreshedDate]
	,[PlacementMonth], 
	,[PlacementId]
	,[CostMethod]
	,1
	,[Invalid 1x1 Placements]
	,[Placements without Delivery]
	,[NA Placements]
	,[Invalid1by1PlannedCost]
	,[PlannedCostWithoutDelivery]
	,[NAPlannedCost]
	,[PlannedAmount]
	,1
	,'c'
FROM  [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_Campaigns_And_Placements_Tables_Combined]
WHERE 
	[BuyType] in ('Display','Search') and PackageType in ('Child','Standalone') and [PlannedDataExists] = 1 and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date


DELETE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
WHERE AgencyName = 'CATALYST' and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date

DELETE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
WHERE MasterClientName like ('%MAZDA%') and Country = 'US' and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date


--3/28 pp: added logic to delete campaignpublicids from Overrride table that are not in compliance  
delete from [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined]
where convert(varchar(10),[Refreshed_Date],112) like @refreshed_date and CampaignPublicId in 
( 
select distinct CampaignPublicId from  [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined] WHERE RecordType = 'o'
except 
select distinct CampaignPublicId from  [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Report_Final_BOD_And_Placements_Combined] WHERE RecordType = 'c'
) 



GO


--TODO: Final check these two renamings
--Compliance_Campaigns_Placements_Placement_Monthly_Final => Compliance_All_Prisma_Tables_Combined
--[CampaignCreationUser]=>[CampaignUser]
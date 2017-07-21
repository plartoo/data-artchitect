USE [DM_1305_GroupMBenchmarkingUS];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Modified Date: 06/21/2017
-- Description:	This stored procedure appends data with specific date range
-- from Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails table  
-- into the final table.
-- =========================================================================


CREATE PROC [dbo].[COMPLIANCE_STEP6_APPEND_PLACEMENTS_DATA_TO_FINAL_TABLE]  AS
BEGIN

SET ARITHABORT OFF;
SET ARITHIGNORE ON;

IF OBJECT_ID('Compliance_Campaigns_And_All_Placements_Tables_Combined') IS NULL
	CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_And_All_Placements_Tables_Combined]
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
		,[CampaignId] BIGINT
		,[CampaignPublicId] NVARCHAR(4000)
		,[CampaignName] NVARCHAR(4000)
		,[PackageType] NVARCHAR(4000)
		,[PackageId] BIGINT
		,[PlacementId] BIGINT
		,[ParentId] BIGINT
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
		,[RefreshedDate] SMALLDATETIME
	)

-- Append 2017 US AND CANADA monthly data 
INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_And_All_Placements_Tables_Combined]
SELECT 
      [AgencyName]
	  ,[AgencyAlphaCode]
      ,[LocationCompanyCode]
      ,[CampaignStartDate]
      ,[CampaignEndDate]
      ,[CampaignStatus]
      ,[RateType]
      ,[Budget]
      ,[BudgetApproved]
      ,[CampaignUser]
	  
      ,[MediaCode]
      ,[MediaName]
      ,[AdvertiserCode]
      ,[AdvertiserName]
      ,[ProductCode]
      ,[ProductName]
      ,[EstimateCode]
      ,[EstimateName]
      ,[EstimateStartDate]
      ,[EstimateEndDate]
      ,[SupplierCode]
      ,[SupplierName]
      ,[BuyType]
      ,[BuyCategory]
      ,[CampaignId]
      ,[CampaignPublicId]
      ,[CampaignName]
      ,[PackageType]
      ,[PackageId]
      ,[PlacementId]
      ,[ParentId]
      ,[PlacementName]
      ,[PlacementType]
      ,[Site]
      ,[Dimension]
      ,[Positioning]
      ,[CostMethod]
      ,[UnitType]
      ,[Rate]
      ,[IONumber]
      ,[ServedBy]
	  ,[AdserverName]
	  ,[AdserverSupplierName]

	  ,[New_PlacementMonth]
      ,[PlacementMonth]
      ,[PlacementMonthlyStartDate]
      ,[PlacementMonthlyEndDate]
      ,[PlannedAmount]
      ,[PlannedUnits]
      ,[PlannedImpressions]
      ,[PlannedClicks]
      ,[PlannedActions]
      ,[IOAmount]
      ,[SupplierUnits]
      ,[SupplierImpressions]
      ,[SupplierClicks]
      ,[SupplierActions]
      ,[SupplierCost]
      ,[AdserverUnits]
      ,[AdserverImpressions]
      ,[AdserverClicks]
      ,[AdserverActions]
      ,[AdserverCost]
      ,[DeliveryExists]
      ,[MasterClientName]
      ,[ChildCount]

      ,[AD FORMAT FOR NON-1X1 UNITS]
      ,[Channel Type 1]
      ,[BUY TYPE 2]
      ,[Channel Type 2]
      ,[Rich Media Types - Format]
      ,[RICH MEDIA/4TH PARTY VENDOR]
      ,[TARGETING TYPE 2]
      ,[BUY TYPE]
      ,[TARGETING TYPE 1]
      ,[Targeting Audience Type]
      ,[Targeting Delivery Type]
      ,[CPA KPI]
      ,[SECONDARY CONTENT CHANNEL]
      ,[TRACKING METHOD]
      ,[Targeting Context Type]
      ,[PRIMARY CONTENT CHANNEL]
      ,[CONTENT CHANNEL DETAILS]
	  ,CONVERT(DATE, DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, GETDATE()), 0))) AS [RefreshedDate]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails]
--WHERE YEAR([PlacementMonthlyStartDate])=2017
-- Pavani also appends 2016 data until march monthly refresh-- which is q1 2017 
END

GO



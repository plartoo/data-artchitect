USE [DM_1305_GroupMBenchmarkingUS];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Modified Date: 06/19/2017
-- Description:	This stored procedure joins Compliance_Campaigns_Placements_Placement_Monthly
-- table (separated into two temp tables: one having all Parent-Child placements 
-- and the other having other placements) with AdvancePlacementDetails table, 
-- bringing in about 17 columns from the latter.
-- =========================================================================

CREATE PROC [dbo].[COMPLIANCE_STEP4_JOIN_ALL_PRISMA_TABLES]  AS
BEGIN

IF OBJECT_ID('Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails]

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
      ,stg.[PlacementId]
      ,[ParentId]
      ,[PlacementName]
      ,[PlacementType]
      --,[PlacementCategory] -- we don't have this in our Marketplace PRISMA tables
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
	  ,[PlacementYear]
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

	  ,cus.[AD FORMAT FOR NON-1X1 UNITS]
      ,cus.[Channel Type 1]
      ,cus.[BUY TYPE 2]
      ,cus.[Channel Type 2]
      ,cus.[Rich Media Types - Format]
      ,cus.[RICH MEDIA/4TH PARTY VENDOR]
      ,cus.[TARGETING TYPE 2]
      ,cus.[BUY TYPE]
      ,cus.[TARGETING TYPE 1]
      ,cus.[Targeting Audience Type]
      ,cus.[Targeting Delivery Type]
      ,cus.[CPA KPI]
      ,cus.[SECONDARY CONTENT CHANNEL]
      ,cus.[TRACKING METHOD]
      ,cus.[Targeting Context Type]
      ,cus.[PRIMARY CONTENT CHANNEL]
      ,cus.[CONTENT CHANNEL DETAILS]
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types] stg
LEFT JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails] cus
	ON stg.[PlacementId]=cus.[PlacementId]

UNION

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
      ,stg.[PlacementId]
      ,[ParentId]
      ,[PlacementName]
      ,[PlacementType]
      --,[PlacementCategory] -- we don't have this in our Marketplace PRISMA tables
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
	  ,[PlacementYear]
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

	  ,cus.[AD FORMAT FOR NON-1X1 UNITS]
      ,cus.[Channel Type 1]
      ,cus.[BUY TYPE 2]
      ,cus.[Channel Type 2]
      ,cus.[Rich Media Types - Format]
      ,cus.[RICH MEDIA/4TH PARTY VENDOR]
      ,cus.[TARGETING TYPE 2]
      ,cus.[BUY TYPE]
      ,cus.[TARGETING TYPE 1]
      ,cus.[Targeting Audience Type]
      ,cus.[Targeting Delivery Type]
      ,cus.[CPA KPI]
      ,cus.[SECONDARY CONTENT CHANNEL]
      ,cus.[TRACKING METHOD]
      ,cus.[Targeting Context Type]
      ,cus.[PRIMARY CONTENT CHANNEL]
      ,cus.[CONTENT CHANNEL DETAILS]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Other_Pkg_Types] stg
LEFT JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails] cus
ON stg.[PlacementId] = cus.[PlacementId]

-- Update MasterClientName from B_clients_ddstomi on mi_target
UPDATE pc
SET pc.[MasterClientName] = bc.[mi_master_client_name]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056630_Compliance_MasterClientName_US_Mappings_Extracted] bc
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails] pc
ON bc.[src_client_code] = pc.[AdvertiserCode]
AND bc.[Src_media_category_code] = 'print'
AND bc.[Src_media_code] = pc.[MediaCode]
AND bc.[Src_media_code] IN ('I','S','L')
AND bc.Src_agency_code = 'h7'

-- Update MasterClientName for Canada data from CA_MASTER_CLIENT_LKP table 
UPDATE pc
SET pc.[MasterClientName] = ca.[master_client]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056631_Compliance_MasterClientName_CA_Mappings_Extracted] ca
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails] pc
ON ca.[Client] = pc.[AdvertiserName]
AND pc.[AgencyAlphaCode] <> 'H7'

-- Bring cost method from parent to child package  
UPDATE a
SET a.[CostMethod] = b.[CostMethod]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails] a
INNER JOIN (SELECT * FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails] WHERE [PackageType]='package') b
	ON a.[ParentId] = b.[ParentId]
	AND a.[New_PlacementMonth] = b.[New_PlacementMonth]
	AND a.[PackageType] = 'Child'
END

GO

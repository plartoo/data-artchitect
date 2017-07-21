USE [DM_1305_GroupMBenchmarkingUS];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Create date: 06/16/2017
-- Description:	This stored procedure joins Campaigns, Placements and 
-- PlacementMonthly tables and allocate parents' Planned metrics and 
-- IOAmount to children.
-- =========================================================================

CREATE PROC [dbo].[COMPLIANCE_STEP3_JOIN_CAMPAIGN_AND_PLACEMENT_TABLES]  AS
BEGIN

IF OBJECT_ID('Compliance_Campaigns_Placements_Placement_Monthly') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]

 -- Insert child packages
 CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]
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
)
-- Insert non-child packages

INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]
SELECT
	 c.[AgencyName]
	,c.[AgencyAlphaCode]
	,c.[LocationCompanyCode]
	,c.[CampaignStartDate]
	,c.[CampaignEndDate]
	,c.[CampaignStatus]
	,c.[RateType]
	,c.[Budget]
	,c.[BudgetApproved]
	,c.[CampaignUser]

	,p.[MediaCode]
	,p.[MediaName]
	,p.[AdvertiserCode]
	,p.[AdvertiserName]
	,p.[ProductCode]
	,p.[ProductName]
	,p.[EstimateCode]
	,p.[EstimateName]
	,p.[EstimateStartDate]
	,p.[EstimateEndDate]
	,p.[SupplierCode]
	,p.[SupplierName]
	,p.[BuyType]
	,p.[BuyCategory]
	,p.[CampaignId]
	,p.[CampaignPublicId]
	,p.[CampaignName]
	,p.[PackageType]
	,p.[PackageId]
	,p.[PlacementId]
	,COALESCE(p.[PackageId],p.[PlacementId]) AS [ParentId]
	,p.[PlacementName]
	,p.[PlacementType]
	--, p.[PlacementCategory] -- we don't have this in Marketplace feed
	,p.[Site]
	,p.[Dimension]
	,p.[Positioning]
	,p.[CostMethod]
	,p.[UnitType]
	,p.[Rate]
	,p.[IONumber]
	,p.[ServedBy]
	,p.[AdserverName]
	,p.[AdserverSupplierName]

	-- combine whatever raw month and year columns are into smalldatetime; if either of the columns in raw data are NULL, default to NULL
    ,CASE 
        WHEN (
				(
					[PlacementMonth] IS NULL
				) 
            OR  (
                [PlacementYear] IS NULL
				)
        ) THEN NULL  
        ELSE CONVERT(SMALLDATETIME, CONCAT(CONVERT(VARCHAR, [PlacementMonth]), '-01-', CONVERT(VARCHAR, [PlacementYear])))
    END AS [New_PlacementMonth]
	,pm.[PlacementMonth]	
	,pm.[PlacementYear]
	,pm.[PlacementMonthlyStartDate]	
	,pm.[PlacementMonthlyEndDate]	
	,pm.[PlannedAmount]	
	,pm.[PlannedUnits]	
	,pm.[PlannedImpressions]	
	,pm.[PlannedClicks]	
	,pm.[PlannedActions]	
	,pm.[IOAmount]	
	,pm.[SupplierUnits]	
	,pm.[SupplierImpressions]	
	,pm.[SupplierClicks]	
	,pm.[SupplierActions]	
	,pm.[SupplierCost]	
	,pm.[AdserverUnits]	
	,pm.[AdserverImpressions]	
	,pm.[AdserverClicks]	
	,pm.[AdserverActions]	
	,pm.[AdserverCost]	
	,pm.[DeliveryExists]
	,NULL
	,NULL
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054934_Prisma_Campaign_Details_Extracted] c
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054937_Prisma_Placement_Details_Extracted] p 
	ON p.[CampaignId] = c.[CampaignId]
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054941_Prisma_Placements_Monthly_Extracted] pm 
	ON pm.[PlacementId] = p.[PlacementId]
WHERE p.[PackageType] <> 'Child'
	AND pm.[PlannedUnits] IS NOT NULL
	AND p.[BuyType] IN ('Display','Search','Social')
  

 -- Insert child packages
 INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]
 SELECT 
	 c.[AgencyName]
	,c.[AgencyAlphaCode]
	,c.[LocationCompanyCode]
	,c.[CampaignStartDate]
	,c.[CampaignEndDate]
	,c.[CampaignStatus]
	,c.[RateType]
	,c.[Budget]
	,c.[BudgetApproved]
	,c.[CampaignUser]
	
	,p.[MediaCode]
	,p.[MediaName]
	,p.[AdvertiserCode]
	,p.[AdvertiserName]
	,p.[ProductCode]
	,p.[ProductName]
	,p.[EstimateCode]
	,p.[EstimateName]
	,p.[EstimateStartDate]
	,p.[EstimateEndDate]
	,p.[SupplierCode]
	,p.[SupplierName]
	,p.[BuyType]
	,p.[BuyCategory]
	,p.[CampaignId]
	,p.[CampaignPublicId]
	,p.[CampaignName]
	,p.[PackageType]
	,p.[PackageId]
	,p.[PlacementId]
	,COALESCE(p.[PackageId],p.[PlacementId]) AS [ParentId]
	,p.[PlacementName]
	,p.[PlacementType]
	--, p.[PlacementCategory] -- we don't have this in Marketplace feed
	,p.[Site]
	,p.[Dimension]
	,p.[Positioning]
	,p.[CostMethod]
	,p.[UnitType]
	,p.[Rate]
	,p.[IONumber]
	,p.[ServedBy]
	,p.[AdserverName]
	,p.[AdserverSupplierName]

	-- combine whatever raw month and year columns are into smalldatetime; if either of the columns in raw data are NULL, default to NULL
    ,CASE 
        WHEN (
				(
					[PlacementMonth] IS NULL
				) 
            OR  (
                [PlacementYear] IS NULL
				)
        ) THEN NULL  
        ELSE CONVERT(SMALLDATETIME, CONCAT(CONVERT(VARCHAR, [PlacementMonth]), '-01-', CONVERT(VARCHAR, [PlacementYear])))
    END AS [New_PlacementMonth]
	,pm.[PlacementMonth]	
	,pm.[PlacementYear]
	,pm.[PlacementMonthlyStartDate]	
	,pm.[PlacementMonthlyEndDate]
	,0.00 AS [PlannedAmount]
	,0 AS [PlannedUnits]
	,pm.[PlannedImpressions]	
	,pm.[PlannedClicks]	
	,pm.[PlannedActions]	
	,pm.[IOAmount]	
	,pm.[SupplierUnits]	
	,pm.[SupplierImpressions]	
	,pm.[SupplierClicks]	
	,pm.[SupplierActions]	
	,pm.[SupplierCost]	
	,pm.[AdserverUnits]	
	,pm.[AdserverImpressions]	
	,pm.[AdserverClicks]	
	,pm.[AdserverActions]	
	,pm.[AdserverCost]	
	,pm.[DeliveryExists]
	,NULL
	,NULL
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054934_Prisma_Campaign_Details_Extracted] c
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054937_Prisma_Placement_Details_Extracted] p 
	ON p.[CampaignId] = c.[CampaignId]
LEFT OUTER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054941_Prisma_Placements_Monthly_Extracted] pm 
	ON pm.[PlacementId] = p.[PlacementId]
WHERE p.[PackageType] = 'Child'
	AND (pm.[PlacementMonthlyStartDate] BETWEEN p.[PlacementStartDate] AND p.[PlacementEndDate])
	AND p.[BuyType] IN ('Display','Search','Social')

	
--update MasterClientName from B_clients_ddstomi on mi_target
UPDATE po
SET po.[MasterClientName] = bc.[mi_master_client_name]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056630_Compliance_MasterClientName_US_Mappings_Extracted] bc
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly] po
ON bc.[src_client_code] = po.[AdvertiserCode]
AND bc.[Src_media_category_code] = 'print'
AND bc.[Src_media_code]= po.[MediaCode]
AND bc.[src_media_code] IN ('I','S','L')
AND bc.[Src_agency_code] = 'h7'
AND po.[AgencyAlphaCode] = 'h7'

--  update MasterClientName for Canada data from CA_MASTER_CLIENT_LKP table 
UPDATE po
SET po.[MasterClientName] = ca.[master_client]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056631_Compliance_MasterClientName_CA_Mappings_Extracted] ca
-- here, Pavani's code seem to have a bug by using INNER JOIN [Compliance_BuyOrderDetails] po; possibly a bug
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly] po 
ON ca.[Client] = po.[AdvertiserName]
AND po.[AgencyAlphaCode] <> 'H7'


--Drop temp staging tables to store parent-child and other placements
IF OBJECT_ID('Compliance_Tmp_Parent_Child_Pkg_Types') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types]

IF OBJECT_ID('Compliance_Tmp_Other_Pkg_Types') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Other_Pkg_Types]

--Create temp table for parent and child package types 
SELECT *
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]
WHERE [PackageType] IN ('Child','Package')

--Create temp table for other packaget types
SELECT *
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Other_Pkg_Types]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]
WHERE [PackageType] NOT IN ('Child','Package')

--We'll use CTEs distribute Parent Package's Planned* metrics to related children and store them in a temp table
IF OBJECT_ID('Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed]

;WITH
parent_cte AS (
	SELECT
			[ParentId],[PackageId],[PlacementId],[PackageType],[New_PlacementMonth]
			,SUM([PlannedAmount]) AS [PlannedAmount]
			,SUM([PlannedUnits]) AS [PlannedUnits]
			,SUM([PlannedImpressions]) AS [PlannedImpressions]
			,SUM([PlannedClicks]) AS [PlannedClicks]
			,SUM([PlannedActions]) AS [PlannedActions]
			,SUM([IOAmount]) AS [IOAmount]
			FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types]
			WHERE [PackageType] = 'Package'
	GROUP BY [ParentId],[PackageId],[PlacementId],[PackageType],[New_PlacementMonth]
)
,child_cte AS (
	SELECT 	
			[ParentId],[PackageId],[PackageType],[New_PlacementMonth]
			,COUNT([PlacementId]) child_cnt
			FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types]
			WHERE [PackageType] = 'Child' 
	GROUP BY [ParentId],[PackageId],[PackageType],[New_PlacementMonth]
) 

--INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed]
SELECT 
	child_cte.child_cnt
	,parent_cte.* 
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed]
FROM parent_cte
INNER JOIN child_cte 
	ON parent_cte.[ParentId] = child_cte.[ParentId]
	AND parent_cte.[New_PlacementMonth]=child_cte.[New_PlacementMonth]


-- Update the  table using the derived temp table from  above
UPDATE a
SET  [ChildCount]=b.child_cnt
	,[PlannedAmount]=b.PlannedAmount/nullif(b.child_cnt,0)
	,[PlannedUnits]=round(cast(b.[PlannedUnits] as float)/cast(nullif(b.child_cnt,0)as decimal(19,5)),6)
	,[PlannedImpressions]=	round(cast(b.[PlannedImpressions] as float)/cast(nullif(b.child_cnt,0) as decimal(19,5)),6) 
	,[PlannedClicks]=round(cast(b.[PlannedClicks]as float)/cast(nullif(b.child_cnt,0) as decimal(19,5)),6) 
	,[PlannedActions]=round(cast(b.[PlannedActions]as float)/cast(nullif(b.child_cnt,0) as decimal(19,5)),6)
	,[IOAmount]=round(cast(b.[IOAmount]as float)/cast(nullif(b.child_cnt,0)as decimal(19,5)),6) 
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types] a
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed] b
	ON a.[ParentId] = b.[ParentId]
	AND a.[New_PlacementMonth] = b.[New_PlacementMonth]
	AND a.[PackageType] = 'Child'

END


GO



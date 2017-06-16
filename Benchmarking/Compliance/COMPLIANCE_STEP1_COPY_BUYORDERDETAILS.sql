USE [DM_1305_GroupMBenchmarkingUS];
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:		Phyo Thiha
-- Create date: 06/12/2017
-- Description:	this stored procedure creates a copy of BuyOrderDetails (BOD).
-- Previous version of Compliance report used to join BOD with Campaigns 
-- table, but we no longer need it because the BOD table that we get via
-- DataMarketplace has all the needed info (as opposed to the ODS table
-- which the old Compliance report code relies on)
--
-- Report requirements: H7 spot data for the current and previous standared 
--						calender years with Agency and Master Client Name 
--						Mappings. 
--                       
-- =========================================================================

CREATE PROC [dbo].[COMPLIANCE_STEP1_COPY_BUYORDERDETAILS]  AS

BEGIN

IF OBJECT_ID('Compliance_BuyOrderDetails') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails]

CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails]
(
	  [CampaignId] [INT],
      [CampaignPublicId] NVARCHAR(4000)
      ,[CampaignName] NVARCHAR(4000)
      ,[AdvertiserCode] NVARCHAR(4000)
	  ,[AdvertiserName] NVARCHAR(4000)
      ,[ProductCode] NVARCHAR(4000)
      ,[EstimateCode] NVARCHAR(4000)
      ,[SupplierId] INT
      ,[SupplierName] NVARCHAR(4000)
      ,[SupplierCode] NVARCHAR(4000)
      --,[SupplierZone] NVARCHAR(4000) -- we don't have this in the table via Marketplace
      ,[MediaCode] NVARCHAR(4000)
	  ,[New_BuyMonth] DATE
      ,[BuyMonth] NVARCHAR(4000)
      ,[BuyAmount] FLOAT
      ,[BuyRcnAmount] FLOAT
      ,[BuyType] NVARCHAR(4000)
      ,[BuySendDate] SMALLDATETIME
      ,[BuySource] NVARCHAR(4000)
	  ,[BuyRefNumber] NVARCHAR(4000)
      ,[IsOverride] NVARCHAR(4000)
	  ,[CampaignCreationUser] NVARCHAR(4000) -- in place of campaigns.[user]
	  ,[AgencyName] NVARCHAR(4000)
	  ,[AgencyAlphaCode] NVARCHAR(4000)
	  ,[LocationCompanyCode] NVARCHAR(4000)
	  ,[MasterClientName] NVARCHAR(4000) NULL
)

INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails]
SELECT bo.[CampaignId]
      ,bo.[CampaignPublicId]
      ,bo.[CampaignName]
      ,bo.[AdvertiserCode]
	  ,bo.[AdvertiserName]
      ,bo.[ProductCode]
      ,bo.[EstimateCode]
      ,bo.[SupplierId]
      ,bo.[SupplierName]
      ,bo.[SupplierCode]
      --,bo.[SupplierZone] -- we don't have this in the table via Marketplace
      ,bo.[MediaCode]
	  ,CONVERT(DATE, '1-'+ [BuyMonth]) AS [New_BuyMonth]
      ,bo.[BuyMonth]
      ,bo.[BuyAmount]
      ,bo.[BuyRcnAmount]
      ,bo.[BuyType]
      ,bo.[BuySendDate]
      ,bo.[BuySource]
	  ,bo.[BuyRefNumber]
      ,bo.[IsOverride]
	  ,bo.[CampaignUser] -- in place of campaigns.[user]
	  ,bo.[AgencyName]
	  ,bo.[AgencyAlphaCode]
	  ,bo.[LocationCompanyCode]
	  ,NULL
  FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056241_Prisma_Buy_Order_Details_Extracted] bo
  WHERE bo.[IsOverride] IS NOT NULL
  AND bo.[IsDeleted] = 'False'


-- update MasterClientName for US data from B_clients_ddstomi on mi_target
UPDATE po
SET po.[MasterClientName] = bc.[mi_master_client_name]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056630_Compliance_MasterClientName_US_Mappings_Extracted] bc
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails] po
ON bc.[src_client_code] = po.[AdvertiserCode]
AND bc.[Src_media_category_code] = 'print'
AND bc.[Src_media_code]= po.[MediaCode]
AND bc.[src_media_code] in ('I','S','L')
AND [Src_agency_code] = 'h7'
AND po.[AgencyAlphaCode] = 'h7'


--  update MasterClientName for Canada data from CA_MASTER_CLIENT_LKP table in BI_Projects on DEV 2012 
UPDATE po
SET po.[MasterClientName] = ca.[master_client]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056631_Compliance_MasterClientName_CA_Mappings_Extracted] ca
INNER JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails] po
ON ca.[Client] = po.[AdvertiserName]
AND po.[AgencyAlphaCode] <> 'H7'

END


GO



USE [DM_1305_GroupMBenchmarkingUS];
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =========================================================================
-- Author:			Phyo Thiha
-- Modified Date: 	06/21/2017
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

CREATE PROC [dbo].[COMPLIANCE_STEP5_APPEND_BUYORDERDETAILS_DATA_TO_FINAL_TABLE]  AS

BEGIN

IF OBJECT_ID('Compliance_BuyOrderDetails_Final') IS NULL
	CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails_Final]
	(
		[CampaignId] INT
		,[CampaignPublicId] NVARCHAR(4000)
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
		,[New_BuyMonth] SMALLDATETIME
		,[BuyMonth] NVARCHAR(4000)
		,[BuyAmount] FLOAT
		,[BuyRcnAmount] FLOAT
		,[BuyType] NVARCHAR(4000)
		,[BuySendDate] SMALLDATETIME
		,[BuySource] NVARCHAR(4000)
		,[BuyRefNumber] NVARCHAR(4000)
		,[IsOverride] NVARCHAR(4000)
		,[CampaignUser] NVARCHAR(4000) -- in place of campaigns.[user]
		,[AgencyName] NVARCHAR(4000)
		,[AgencyAlphaCode] NVARCHAR(4000)
		,[LocationCompanyCode] NVARCHAR(4000)
		,[MasterClientName] NVARCHAR(4000) NULL
		,[RefreshedDate] SMALLDATETIME NULL
	);

-- Append monthly data for 2017 for US and Canada from PRISMA API data
INSERT INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails_Final]
SELECT [CampaignId]
      ,[CampaignPublicId]
      ,[CampaignName]
      ,[AdvertiserCode]
      ,[AdvertiserName]
      ,[ProductCode]
      ,[EstimateCode]
      ,[SupplierId]
      ,[SupplierName]
      ,[SupplierCode]
      --,[SupplierZone]
      ,[MediaCode]
	  ,[New_BuyMonth]
      ,[BuyMonth]
      ,[BuyAmount]
      ,[BuyRcnAmount]
      ,[BuyType]
      ,[BuySendDate]
      ,[BuySource]
      ,[BuyRefNumber]
      ,[IsOverride]
      ,[CampaignUser]--[user]
      ,[AgencyName]
      ,[AgencyAlphaCode]
      ,[LocationCompanyCode]
      ,[MasterClientName]
	  -- last day of the most recent month
	  ,CONVERT(DATE, DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, GETDATE()), 0))) AS [Refreshed_Date]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails]
--Pavani's code appends only 2016 and 2017 data like below
--WHERE ([BuyMonth] LIKE '%2017%') 
--WHERE ([BuyMonth] LIKE '%2016%')

END

GO

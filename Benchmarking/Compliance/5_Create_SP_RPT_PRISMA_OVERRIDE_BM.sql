USE [DM_1305_GroupMBenchmarkingUS];
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--exec [SP_Compliance_BuyOrderDetails_BM]

-- =========================================================================
-- Author:		Pavani Pagadala
-- Create date: 04/30/2015
-- Description:	this stored procedure creates Compliance_BuyOrderDetails_BM Table
-- Report requirements: H7 Prisma API data for the current and last year standard years.
-- 4/30/2015 - changed to append 4/30 Prisma dataset for 2015
-- 5/30/2015 - changed to append 5/30 prisma data for 2015 and appended 2014 prisma dateset from baseline tables 
-- 6/30/2015 - changed to append 6/30 prisma data for 2015 and appended 2015 3/20 dataset from 2014 baseline tables 
-- 9/22/2015 - Replaced 2014 US baseline data with 2014-2015 US and CANADA API data 
-- 10/06/2014-- Replaced 2014-2015 US and Canada Data with the virtual delete logic 
--02/01/2016 -- changed date filter to include 2016 data and modified logc to append 2015 data under 2016-01-30 refreshed_date timestamp
--05/01/2017 pp: commented out appending 2016 data as we should only do that for the 1st quarter of 2017 
--05/30/2017 PP: Uncommented 2016 data append per Greg'+Margaret's request 
-- =========================================================================


CREATE PROC [dbo].[SP_Compliance_BuyOrderDetails_BM]  AS

BEGIN

  ----append monthly data for 2017 for US and Canada from prisma APi data 
 INSERT INTO [dbo].[Compliance_BuyOrderDetails_2015_BM]
 SELECT [CampaignId]
      ,[CampaignPublicId]
      ,[CampaignName]
      ,[AdvertiserCode]
      ,[AdvertiserName]
      ,[ProductCode]
      ,[EstimateCode]--
      ,[SupplierId]
      ,[SupplierCode]
      ,[SupplierZone]
      ,[SupplierName]
      ,[MediaCode]
      ,[BuyMonth]
      ,[BuyType]
      ,[BuySource]
      ,[BuyAmount]
      ,[BuyRcnAmount]
      ,[IsOverride]
      ,[BuySendDate]
      ,[BuyRefNumber]
      ,[user]
      ,[AgencyAlphaCode]
      ,[AgencyName]
      ,[LocationCompanyCode]
      ,[MasterClientName]
	  ,'2017-05-30 00:00:00'[Refreshed_Date]
  FROM [dbo].[Compliance_BuyOrderDetails]
  where (buymonth like '%2017%'
  --and AgencyAlphaCode not in ('h7')  --and AgencyAlphaCode in ('h7','h0')
  )
   

  --  -- append 2016 data until march monthly refresh-- which is q1 2017 
  -- we need to append 2016 data until June 2017 - new request from Margaret
  
  INSERT INTO [dbo].[Compliance_BuyOrderDetails_2015_BM]
   SELECT [CampaignId]
      ,[CampaignPublicId]
      ,[CampaignName]
      ,[AdvertiserCode]
      ,[AdvertiserName]
      ,[ProductCode]
      ,[EstimateCode]--
      ,[SupplierId]
      ,[SupplierCode]
      ,[SupplierZone]
      ,[SupplierName]
      ,[MediaCode]
      ,[BuyMonth]
      ,[BuyType]
      ,[BuySource]
      ,[BuyAmount]
      ,[BuyRcnAmount]
      ,[IsOverride]
      ,[BuySendDate]
      ,[BuyRefNumber]
      ,[user]
      ,[AgencyAlphaCode]
      ,[AgencyName]
      ,[LocationCompanyCode]
      ,[MasterClientName]
	  ,'2017-05-30 00:00:00'[Refreshed_Date]
  FROM [dbo].[Compliance_BuyOrderDetails]
  where (buymonth like '%2016%')

  


END

GO



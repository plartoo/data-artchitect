
/****** Object:  StoredProcedure [dbo].[SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM]    Script Date: 6/1/2017 11:04:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





--exec [SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM] --00:9:30 on 1/3/2017
-- =========================================================================
-- Author:		Pavani Pagadala
-- Create date: 04/30/2015
-- Description:	this stored procedure creates RPT_PRISMA_COMPLIANCE_2015_BMTable
-- Report requirements: H7 Prisma API data for the current and last year standard years.
-- 4/30/2015 - changed to append 4/30 Prisma dataset for 2015
-- 5/30/2015 - changed to append 5/30 prisma data for 2015 and appended 2014 prisma dateset from baseline tables 
-- 6/30/2015 - changed to append 6/30 prisma data for 2015 and appended 2015 3/20 dataset from 2014 baseline tables 
-- 9/22/2015 - Replaced 2014 US baseline data with 2014-2015 US and CANADA API data and incorpoarted new MO logic for virtual deletes
-- 10/06/2014 - Replaced 2014-2015 US and Canada Data with the virtual delete logic 
---03/03/2016 - ran a Bulk update to replace cost method values on child pkg from Parent pkg
-- 12/02/2016 - Added scripots to include 2016 and 2017 data and commented them . need to uncomment for 1/30/2017 refresh
--05/01/2017 pp: commented out appending 2016 data as we should only do that for the 1st quarter of 2017 
--05/30/2017 PP: Uncommented 2016 data append per Greg'+Margaret's request 
-- =========================================================================

CREATE PROC [dbo].[SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM]  AS

BEGIN

SET ARITHABORT OFF;
SET ARITHIGNORE ON;


--- Append 2015 H7 data and (2014 + 2015) H0 data -- done on 9/1 

--insert into [dbo].[RPT_PRISMA_COMPLIANCE_2015_BM]
--SELECT [AgencyAlphaCode]
--      ,[AgencyName]
--      ,[LocationCompanyCode]
--      ,[CampaignStartDate]
--      ,[CampaignEndDate]
--      ,[CampaignStatus]
--      ,[RateType]
--      ,[Budget]
--      ,[BudgetApproved]
--      ,[User]
--      ,[MediaCode]
--      ,[MediaName]
--      ,[AdvertiserCode]
--      ,[AdvertiserName]
--      ,[ProductCode]
--      ,[ProductName]
--      ,[EstimateCode]
--      ,[EstimateName]
--      ,[EstimateStartDate]
--      ,[EstimateEndDate]
--      ,[SupplierCode]
--      ,[SupplierName]
--      ,[BuyType]
--      ,[BuyCategory]
--      ,[CampaignId]
--      ,[CampaignPublicId]
--      ,[CampaignName]
--      ,[PackageType]
--      ,[PackageId]
--      ,[PlacementId]
--      ,[ParentId]
--      ,[PlacementName]
--      ,[PlacementType]
--      ,[PlacementCategory]
--      ,[Site]
--      ,[Dimension]
--      ,[Positioning]
--      ,[CostMethod]
--      ,[UnitType]
--      ,[Rate]
--      ,[IONumber]
--      ,[ServedBy]
--      ,[PlacementMonth]
--      ,[PlacementMonthlyStartDate]
--      ,[PlacementMonthlyEndDate]
--      ,[PlannedAmount]
--      ,[PlannedUnits]
--      ,[PlannedImpressions]
--      ,[PlannedClicks]
--      ,[PlannedActions]
--      ,[IOAmount]
--      ,[SupplierUnits]
--      ,[SupplierImpressions]
--      ,[SupplierClicks]
--      ,[SupplierActions]
--      ,[SupplierCost]
--      ,[AdserverUnits]
--      ,[AdserverImpressions]
--      ,[AdserverClicks]
--      ,[AdserverActions]
--      ,[AdserverCost]
--      ,[DeliveryExists]
--      ,[MasterClientName]
--      ,[ChildCount]
--      ,[AD FORMAT FOR NON-1X1 UNITS]
--      ,[Channel Type 1]
--      ,[BUY TYPE 2]
--      ,[Channel Type 2]
--      ,[Rich Media Types - Format]
--      ,[RICH MEDIA/4TH PARTY VENDOR]
--      ,[TARGETING TYPE 2]
--      ,[BUY TYPE]
--      ,[TARGETING TYPE 1]
--      ,[Targeting Audience Type]
--      ,[Targeting Delivery Type]
--      ,[CPA KPI]
--      ,[SECONDARY CONTENT CHANNEL]
--      ,[TRACKING METHOD]
--      ,[Targeting Context Type]
--      ,[PRIMARY CONTENT CHANNEL]
--      ,[CONTENT CHANNEL DETAILS]
--	  ,'2015-08-30 00:00:00' [Refreshed_Date]
--	  ,[AdserverName]
--	  ,[AdserverSupplierName]
--	  from [Compliance_Placements_PlacementMonthly_AdvPlacementDetails]
--	  where AgencyAlphaCode='h7' and year(CampaignStartDate) in (2015)


-- append 2015 US AND CANADA monthly data 
--insert into [dbo].[RPT_PRISMA_COMPLIANCE_2015_BM]
--SELECT [AgencyAlphaCode]
--      ,[AgencyName]
--      ,[LocationCompanyCode]
--      ,[CampaignStartDate]
--      ,[CampaignEndDate]
--      ,[CampaignStatus]
--      ,[RateType]
--      ,[Budget]
--      ,[BudgetApproved]
--      ,[User]
--      ,[MediaCode]
--      ,[MediaName]
--      ,[AdvertiserCode]
--      ,[AdvertiserName]
--      ,[ProductCode]
--      ,[ProductName]
--      ,[EstimateCode]
--      ,[EstimateName]
--      ,[EstimateStartDate]
--      ,[EstimateEndDate]
--      ,[SupplierCode]
--      ,[SupplierName]
--      ,[BuyType]
--      ,[BuyCategory]
--      ,[CampaignId]
--      ,[CampaignPublicId]
--      ,[CampaignName]
--      ,[PackageType]
--      ,[PackageId]
--      ,[PlacementId]
--      ,[ParentId]
--      ,[PlacementName]
--      ,[PlacementType]
--      ,[PlacementCategory]
--      ,[Site]
--      ,[Dimension]
--      ,[Positioning]
--      ,[CostMethod]
--      ,[UnitType]
--      ,[Rate]
--      ,[IONumber]
--      ,[ServedBy]
--      ,[PlacementMonth]
--      ,[PlacementMonthlyStartDate]
--      ,[PlacementMonthlyEndDate]
--      ,[PlannedAmount]
--      ,[PlannedUnits]
--      ,[PlannedImpressions]
--      ,[PlannedClicks]
--      ,[PlannedActions]
--      ,[IOAmount]
--      ,[SupplierUnits]
--      ,[SupplierImpressions]
--      ,[SupplierClicks]
--      ,[SupplierActions]
--      ,[SupplierCost]
--      ,[AdserverUnits]
--      ,[AdserverImpressions]
--      ,[AdserverClicks]
--      ,[AdserverActions]
--      ,[AdserverCost]
--      ,[DeliveryExists]
--      ,[MasterClientName]
--      ,[ChildCount]
--      ,[AD FORMAT FOR NON-1X1 UNITS]
--      ,[Channel Type 1]
--      ,[BUY TYPE 2]
--      ,[Channel Type 2]
--      ,[Rich Media Types - Format]
--      ,[RICH MEDIA/4TH PARTY VENDOR]
--      ,[TARGETING TYPE 2]
--      ,[BUY TYPE]
--      ,[TARGETING TYPE 1]
--      ,[Targeting Audience Type]
--      ,[Targeting Delivery Type]
--      ,[CPA KPI]
--      ,[SECONDARY CONTENT CHANNEL]
--      ,[TRACKING METHOD]
--      ,[Targeting Context Type]
--      ,[PRIMARY CONTENT CHANNEL]
--      ,[CONTENT CHANNEL DETAILS]
--	  ,'2015-12-30 00:00:00' [Refreshed_Date]
--	  ,[AdserverName]
--	  ,[AdserverSupplierName]
--	  from [Compliance_Placements_PlacementMonthly_AdvPlacementDetails]
--	  where 
--	  --AgencyAlphaCode<>'h7' and 
--	  year([PlacementMonthlyStartDate])=2015
/*


insert into [dbo].[RPT_PRISMA_COMPLIANCE_2015_BM]
SELECT [AgencyAlphaCode]
      ,[AgencyName]
      ,[LocationCompanyCode]
      ,[CampaignStartDate]
      ,[CampaignEndDate]
      ,[CampaignStatus]
      ,[RateType]
      ,[Budget]
      ,[BudgetApproved]
      ,[User]
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
      ,[PlacementCategory]
      ,[Site]
      ,[Dimension]
      ,[Positioning]
      ,[CostMethod]
      ,[UnitType]
      ,[Rate]
      ,[IONumber]
      ,[ServedBy]
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
	  ,'2014-12-31 00:00:00' [Refreshed_Date]
	  ,[AdserverName]
	  ,[AdserverSupplierName]
	  from [Compliance_Placements_PlacementMonthly_AdvPlacementDetails]
	  where 
	  --AgencyAlphaCode ='h7' and 
	  --year(CampaignStartDate) in (2014) and 
	  year([PlacementMonthlyStartDate])=2014


*/
-- append 2017 US AND CANADA monthly data 
insert into [dbo].[RPT_PRISMA_COMPLIANCE_2015_BM]
SELECT [AgencyAlphaCode]
      ,[AgencyName]
      ,[LocationCompanyCode]
      ,[CampaignStartDate]
      ,[CampaignEndDate]
      ,[CampaignStatus]
      ,[RateType]
      ,[Budget]
      ,[BudgetApproved]
      ,[User]
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
      ,[PlacementCategory]
      ,[Site]
      ,[Dimension]
      ,[Positioning]
      ,[CostMethod]
      ,[UnitType]
      ,[Rate]
      ,[IONumber]
      ,[ServedBy]
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
	  ,'2017-05-30 00:00:00' [Refreshed_Date]
	  ,[AdserverName]
	  ,[AdserverSupplierName]
	  from [Compliance_Placements_PlacementMonthly_AdvPlacementDetails]
	  where 
	  --AgencyAlphaCode<>'h7' and 
	  year([PlacementMonthlyStartDate])=2017

	  
---- append 2016 data until march monthly refresh-- which is q1 2017 
-- 05/30/2017 :Re-enabled 2016 data append per Greg'+Margaret's request 

insert into [dbo].[RPT_PRISMA_COMPLIANCE_2015_BM]
SELECT [AgencyAlphaCode]
      ,[AgencyName]
      ,[LocationCompanyCode]
      ,[CampaignStartDate]
      ,[CampaignEndDate]
      ,[CampaignStatus]
      ,[RateType]
      ,[Budget]
      ,[BudgetApproved]
      ,[User]
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
      ,[PlacementCategory]
      ,[Site]
      ,[Dimension]
      ,[Positioning]
      ,[CostMethod]
      ,[UnitType]
      ,[Rate]
      ,[IONumber]
      ,[ServedBy]
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
	  ,'2017-05-30 00:00:00' [Refreshed_Date]
	  ,[AdserverName]
	  ,[AdserverSupplierName]
	  from [Compliance_Placements_PlacementMonthly_AdvPlacementDetails]
	  where 
	  --AgencyAlphaCode<>'h7' and 
	  year([PlacementMonthlyStartDate])=2016
	  
/*

Adhoc canada master client updates for Overrride and Compliance table

update po
set po.[MasterClientName]=CA.master_client
from  
[dbo].[CA_MASTER_CLIENT_LKP] CA,Compliance_BuyOrderDetails_2015_BM po
where
ca.Client=po.[AdvertiserName]
and po.AgencyAlphaCode='H0'


update pc
set pc.[MasterClientName]=CA.master_client
from  
[dbo].[CA_MASTER_CLIENT_LKP] CA,[RPT_PRISMA_COMPLIANCE_2015_BM] pc
where
ca.Client=pc.[AdvertiserName]
and pc.AgencyAlphaCode='H0'



Adhoc US data  master client updates for Overrride and Compliance table
--update po
set po.[MasterClientName]=bc.mi_master_client_name
--select  po.[MasterClientName],bc.mi_master_client_name
from  
[MI_Target].[dbo].[B_CLIENTS_DDSTOMI] bc, RPT_PRISMA_COMPLIANCE_2015_BM po
where
bc.src_client_code=po.[AdvertiserCode]
and bc.[Src_media_category_code]='print'
and bc.Src_media_code=po.MediaCode
and bc.src_media_code in ('I','S','L')
and Src_agency_code='h7'
and po.AgencyAlphaCode='h7'

--update po
set po.[MasterClientName]=bc.mi_master_client_name
--select  po.[MasterClientName],bc.mi_master_client_name
from  
[MI_Target].[dbo].[B_CLIENTS_DDSTOMI] bc,Compliance_BuyOrderDetails_2015_bm  po
where
bc.src_client_code=po.[AdvertiserCode]
and bc.[Src_media_category_code]='print'
and bc.Src_media_code=po.MediaCode
and bc.src_media_code in ('I','S','L')
and Src_agency_code='h7'
and po.AgencyAlphaCode='h7'

*/

/*
---03/03/206 pp: Bulk update to replace cost method on child pkg from Parent pkg
-- select distinct refreshed_date from RPT_PRISMA_COMPLIANCE_2015_BM
update a
set a.costmethod=b.costmethod
from [NA_Digital_Trading_Stg].dbo.RPT_PRISMA_COMPLIANCE_2015_BM a,( select * from 
[NA_Digital_Trading_Stg].dbo.RPT_PRISMA_COMPLIANCE_2015_BM where packagetype='package')b
where 
--a. packagetype=b.packagetype and
a.parentid=b.parentid and
--a.placementid=b.placementid and 
--a.packageid=b.packageid and
a.refreshed_date=b.refreshed_date and
a.placementMonth=b.placementMonth and
a.refreshed_date=b.refreshed_date and
--a.parentid=2719537 and 
a.packagetype='Child' and
a.refreshed_date = '2016-03-30 00:00:00.000'
*/

END
















GO




/****** Object:  StoredProcedure [dbo].[SP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]    Script Date: 6/1/2017 11:10:27 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--  on 4/4/2016 it took 5 hours and 47 min to complete!
-- on 7/14/2016 took 8 hrs 49 mins to complete
-- on 8/1/2016 took 3 hrs 45 mins to complete
--exec [dbo].[PP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]

CREATE PROCEDURE [dbo].[SP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]
AS

--exec [SP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]

-- =========================================================================
-- Author:		Pavani Pagadala
-- Create date: 04/15/2016
-- Description:	this stored procedure creates [ALEX_RPT_PRISMA_SUMMARY_2015] Table
-- Report requirements: US and Canada data for the current and previous standared calender years.
--                      with Agency and Master client Name Mappings (This SP is revised version of Alex's SP
--						ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE)
-- 08/27/2015: Modified to pull Canada data and add master client and Agency lookups 
-- 05/26/2016: Modified missing delivery logic and creative Dimension formulae requested by Margaret
-- 07/14/2016: Modified logic to include added value placements ( costmethod='free' and PlannedCost=0)   
--01/03/2017 : changed SP name from  [PP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE] to  
--              [SP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]                  
-- =========================================================================


declare @refreshed_date varchar(10)
set @refreshed_date='201705%'



insert into [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]	(
[AgencyAlphaCode],[AgencyName],[LocationCompanyCode],[CampaignStartDate],[CampaignEndDate],[CampaignStatus],[RateType],[Budget],[BudgetApproved],
	[User], [MediaCode],[MediaName],[AdvertiserCode],[AdvertiserName],[ProductCode],[ProductName],[EstimateCode],[EstimateName],[EstimateStartDate],
	[EstimateEndDate],[SupplierCode],[SupplierName],[BuyType],[BuyCategory],[CampaignId],[CampaignPublicId],[CampaignName],[PackageType],[PackageId],
	[PlacementId],[ParentId],[PlacementName],[PlacementType],[PlacementCategory],[Site],[Dimension],[Positioning],[CostMethod],[UnitType],[Rate],[IONumber],
	[ServedBy],[PlacementMonth],[PlacementMonthlyStartDate],[PlacementMonthlyEndDate],[PlannedAmount],[PlannedUnits],[PlannedImpressions],[PlannedClicks],[PlannedActions],
	[IOAmount],[SupplierUnits],[SupplierImpressions],[SupplierClicks],[SupplierActions],[SupplierCost],[AdserverUnits],[AdserverImpressions],[AdserverClicks],
	[AdserverActions],[AdserverCost],[DeliveryExists],[MasterClientName],[ChildCount],[AD FORMAT FOR NON-1X1 UNITS],[Channel Type 1],[BUY TYPE 2],
	[Channel Type 2],[Rich Media Types - Format],[RICH MEDIA/4TH PARTY VENDOR],[TARGETING TYPE 2],[BUY TYPE],[TARGETING TYPE 1],[Targeting Audience Type],
	[Targeting Delivery Type],[CPA KPI],[SECONDARY CONTENT CHANNEL],[TRACKING METHOD],[Targeting Context Type],[PRIMARY CONTENT CHANNEL],[CONTENT CHANNEL DETAILS],
	[Refreshed_Date],
	[1By1Acceptable],[1X1Exists],[CONTRY],[DataDeliveryExists],
	[PlacementYear],
	[MonthShort],[PlannedDataExists],[Record],
	[NA Column Count])
SELECT [AgencyAlphaCode],[AgencyName],[LocationCompanyCode],[CampaignStartDate],[CampaignEndDate],[CampaignStatus],[RateType],[Budget],[BudgetApproved],
	[User], [MediaCode],[MediaName],[AdvertiserCode],[AdvertiserName],[ProductCode],[ProductName],[EstimateCode],[EstimateName],[EstimateStartDate],
	[EstimateEndDate],[SupplierCode],[SupplierName],[BuyType],[BuyCategory],[CampaignId],[CampaignPublicId],[CampaignName],[PackageType],[PackageId],
	[PlacementId],[ParentId],[PlacementName],[PlacementType],[PlacementCategory],[Site],[Dimension],[Positioning],[CostMethod],[UnitType],[Rate],[IONumber],
	[ServedBy],[PlacementMonth],[PlacementMonthlyStartDate],[PlacementMonthlyEndDate],[PlannedAmount],[PlannedUnits],[PlannedImpressions],[PlannedClicks],[PlannedActions],
	[IOAmount],[SupplierUnits],[SupplierImpressions],[SupplierClicks],[SupplierActions],[SupplierCost],[AdserverUnits],[AdserverImpressions],[AdserverClicks],
	[AdserverActions],[AdserverCost],[DeliveryExists],[MasterClientName],[ChildCount],[AD FORMAT FOR NON-1X1 UNITS],[Channel Type 1],[BUY TYPE 2],
	[Channel Type 2],[Rich Media Types - Format],[RICH MEDIA/4TH PARTY VENDOR],[TARGETING TYPE 2],[BUY TYPE],[TARGETING TYPE 1],[Targeting Audience Type],
	[Targeting Delivery Type],[CPA KPI],[SECONDARY CONTENT CHANNEL],[TRACKING METHOD],[Targeting Context Type],[PRIMARY CONTENT CHANNEL],[CONTENT CHANNEL DETAILS],
	[Refreshed_Date], 
	CASE 
	WHEN
		(
		(Lower([Positioning]) like '%email%'
		Or Lower([Positioning]) like '%social%'
		Or Lower([Positioning]) like '%value%'
		Or Lower([Positioning]) like '%editorial%'
		Or Lower([Positioning]) like '%logo%'
		Or Lower([Positioning]) like '%text%'
		Or Lower([Positioning]) like '%key word%'
		Or Lower([Positioning]) like '%keyword%'
		Or Lower([Positioning]) like '%billboard%'
		Or Lower([Positioning]) like '%tracking pixel%'
		Or Lower([Positioning]) like '%accounts%'
		Or Lower([Positioning]) like '%advertorial%'
		Or Lower([Positioning]) like '%articles%'
		Or Lower([Positioning]) like '%audio%'
		Or Lower([Positioning]) like '%backdrop%'
		Or Lower([Positioning]) like '%background%'
		Or Lower([Positioning]) like '%blast%'
		Or Lower([Positioning]) like '%blog%'
		Or Lower([Positioning]) like '%blogger%'
		Or Lower([Positioning]) like '%blogvertorial unit%'
		Or Lower([Positioning]) like '%canceled%'
		Or Lower([Positioning]) like '%cancelled%'
		Or Lower([Positioning]) like '%characters%'
		Or Lower([Positioning]) like '%co-brand%'
		Or Lower([Positioning]) like '%eblast%'
		Or Lower([Positioning]) like '%ebook%'
		Or Lower([Positioning]) like '%facebook%'
		Or Lower([Positioning]) like '%fb%'
		Or Lower([Positioning]) like '%fbx%'
		Or Lower([Positioning]) like '%fee%'
		Or Lower([Positioning]) like '%foursquare%'
		Or Lower([Positioning]) like '%header%'
		Or Lower([Positioning]) like '%instagram%'
		Or Lower([Positioning]) like '%link%'
		Or Lower([Positioning]) like '%logo%'
		Or Lower([Positioning]) like '%pinterest%'
		Or Lower([Positioning]) like '%post%'
		Or Lower([Positioning]) like '%print%'
		Or Lower([Positioning]) like '%seeding%'
		Or Lower([Positioning]) like '%skin%'
		Or Lower([Positioning]) like '%sleeve%'
		Or Lower([Positioning]) like '%static wrap%'
		Or Lower([Positioning]) like '%study%'
		Or Lower([Positioning]) like '%survey%'
		Or Lower([Positioning]) like '%tout%'
		Or Lower([Positioning]) like '%tumblr%'
		Or Lower([Positioning]) like '%tweet%'
		Or Lower([Positioning]) like '%twitter%'
		Or Lower([Positioning]) like '%wallpaper%')
		AND (lower([Positioning]) <> 'contextual')
		and (lower([Positioning]) <> 'contextual static/flash')
		) 
		or 
		([ServedBy]='3rd party' and [Dimension]='1 x 1')
		or
		([Dimension]='' and ([PackageType]='FeeOrder' or [PackageType]='Package' or [PackageType]='Roadblock' or [PackageType]='SearchOrder'))
	THEN 'true'
	ELSE 'false'
	END as [1By1Acceptable],
	CASE 
	WHEN 
		[Dimension] is null 
		or [Dimension]= '1 x 1' 
		or lower([Dimension]) = 'n/a' 
		or [Dimension]= '' 
	THEN 'true' 
	ELSE 'false' 
	END as [1X1Exists],
	CASE 
	WHEN 
		[AgencyAlphaCode]='H7' 
	THEN 'US' 
	ELSE 'Canada' 
	END as [CONTRY],
	CASE
	WHEN  
	   CASE WHEN [CostMethod]='Flat' THEN [PlannedAmount] ELSE [AdserverCost] END > 0
		and ([AdserverActions] > 0 
		or [AdserverClicks] > 0 
		or [AdserverImpressions] > 0 
		or [AdserverUnits] > 0) 
	or CASE WHEN [CostMethod]='Flat' THEN [PlannedAmount] ELSE [SupplierCost] END > 0 
		and ([SupplierUnits]>0 
		or [SupplierActions]>0 
		or [SupplierClicks]>0 or [SupplierImpressions]>0 )
	or CASE WHEN ([CostMethod]='Free' and [PlannedAmount] = 0) THEN [PlannedAmount] ELSE [PlannedAmount] END= 0
		   AND
		   ([AdserverActions] > 0 
		   or [AdserverClicks] > 0 
		   or [AdserverImpressions] > 0 
		   or [AdserverUnits] > 0
		   or [SupplierUnits]>0 
		   or [SupplierActions]>0 
		   or [SupplierClicks]>0 
		   or [SupplierImpressions]>0)
	THEN 'true' 
	ELSE 'false' 
	END as [DataDeliveryExists],
	RIGHT([PlacementMonth],4) as [PlacementYear],
	LEFT([PlacementMonth],3) as [MonthShort],
	CASE
	WHEN
		[PlannedAmount] = 0 and [PlannedActions] = 0 and [PlannedClicks] = 0 and
		[PlannedImpressions] = 0 and [PlannedUnits]= 0
	THEN 'false' 
	ELSE 'true' 
	END as [PlannedDataExists],
	CONVERT(nvarchar(30),[PlacementId]) + ' ' + [PlacementMonth] as [Record],
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
FROM [dbo].[RPT_PRISMA_COMPLIANCE_2015_BM]
	  where  convert(varchar(10),[Refreshed_Date],112) like @refreshed_date

--delete records with CampaignStatus = 'Deleted' (deleted campaigns) 
DELETE [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
where  CampaignStatus = 'Deleted' and  convert(varchar(10),[Refreshed_Date],112) like @refreshed_date

DELETE [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
Where Refreshed_Date = '2015-03-30 00:00:00.000' --will be no need in the statement

DELETE [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
Where Refreshed_Date = '2015-08-30 00:00:00.000' --will be no need in the statement

DELETE [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
Where Refreshed_Date = '2015-09-22 00:00:00.000' --will be no need in the statement

--delete records with AgencyName='GARAGE TEAM MAZDA'
DELETE [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
WHERE    UPPER(AgencyName) = 'GARAGE TEAM MAZDA' and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date




--update [1By1IneligiblePlanned Amount], [1By1IneligiblePlannedImpressions], [NA Placements], [Placements without Delivery], [Invalid 1x1 Placements], [Invalid1by1PlannedCost], [PlannedCostWithoutDelivery],
--[NAPlannedCost] columns using logic specified for reporting purposes by Todd Snyder and Jim Mulvey

	UPDATE [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
	SET 
		[1By1IneligiblePlanned Amount] = 
			CASE WHEN [1By1Acceptable] = 0 then [PlannedAmount]
			ELSE NULL END,
		[1By1IneligiblePlannedImpressions] = 
			CASE WHEN [1By1Acceptable] = 0 then [PlannedImpressions]
			ELSE NULL END,
		[NA Placements] = 
			CASE WHEN [NA Column Count] >= 4 THEN 'true' ELSE 'false' END,
		[Placements without Delivery] = 
			CASE WHEN [DataDeliveryExists] = 0 then 'true'  ELSE 'false' END,
		[Invalid 1x1 Placements] = 
			CASE WHEN [1By1Acceptable] = 0 and [1X1Exists] = 1 then 1 ELSE 0 END,
		[Invalid1by1PlannedCost] = 
			CASE  WHEN [1By1Acceptable] = 0 and [1X1Exists] = 1 then PlannedAmount ELSE 0 END,
		[PlannedCostWithoutDelivery] = 
			CASE WHEN [DataDeliveryExists] = 0  then PlannedAmount ELSE 0 END,
		[NAPlannedCost] = 
			CASE WHEN [NA Column Count] >= 4 THEN PlannedAmount ELSE 0 END
	where  convert(varchar(10),[Refreshed_Date],112) like @refreshed_date


-----------------------------------------------------

--insert only records with [BuyType] in ('Display','Search'), exclude records with AgencyName='GARAGE TEAM MAZDA'
INSERT INTO [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015](
[COUNTRY],[AgencyName],[MasterClientName],[AdvertiserName],[AdvertiserCode],[User], [CampaignPublicId],[CampaignName], [Refreshed_Date], [Month], 
	[# of Records],	[# of Buys Overridden], [BuyAmount], [OverrideDollars], [RecordType] )
SELECT 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END as COUNTRY,
	[AgencyName],
	UPPER(ISNull([MasterClientName],'')) as [MasterClientName],
	[AdvertiserName],
	[AdvertiserCode],
	[User],
	[CampaignPublicId],
	[CampaignName],
	[Refreshed_Date],
	[BuyMonth] as [Month],
	COUNT(*) as [# of Records],
	SUM(CONVERT(int,[IsOverride])) as [# of Buys Overridden],
	SUM([BuyAmount]) as [BuyAmount],
	CASE WHEN SUM(CONVERT(int,[IsOverride])) >= 1 THEN SUM([BuyAmount]) ELSE 0 END as [OverrideDollars], 
	'O' as [RecordType]
FROM [dbo].[Compliance_BuyOrderDetails_2015_BM]
WHERE 
	[BuyType] in ('Display','Search') and UPPER(AgencyName) <> 'GARAGE TEAM MAZDA'
	and  convert(varchar(10),[Refreshed_Date],112) like @refreshed_date
GROUP BY 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END,
	[AgencyName],	
	ISNull([MasterClientName],''),
	[AdvertiserName],
	[AdvertiserCode],
	[User],
	--[IsOverride],
	[Refreshed_Date],
	[BuyMonth],
	[CampaignPublicId],
	[CampaignName]
	order by COUNTRY,
	[AgencyName],	
	ISNull([MasterClientName],''),
	[AdvertiserName],
	[AdvertiserCode]

DELETE [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015]
Where Refreshed_Date = '2015-03-30 00:00:00.000'--will be no need in the statement

DELETE [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015]
Where Refreshed_Date = '2015-08-30 00:00:00.000'--will be no need in the statement

DELETE [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015]
Where Refreshed_Date = '2015-09-22 00:00:00.000'--will be no need in the statement


--create temp table to calculate [OverrideDollars] amount
SELECT 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END as COUNTRY,
	[AgencyName],
	UPPER(ISNull([MasterClientName],'')) as [MasterClientName],
	[AdvertiserName],
	[AdvertiserCode],
	[User],
	[CampaignPublicId],
	[CampaignName],
	[Refreshed_Date],
	[BuyMonth], 
	SUM([BuyAmount]) as [OverrideDollars]
into temp_ALEX_RPT_PRISMA_SUMMARY_2015
FROM [dbo].[Compliance_BuyOrderDetails_2015_BM]
WHERE 
	[BuyType] in ('Display','Search') 
	and UPPER([Compliance_BuyOrderDetails_2015_BM].AgencyName) <> 'GARAGE TEAM MAZDA' 
	and [Compliance_BuyOrderDetails_2015_BM].Isoverride = 1
	and  convert(varchar(10),[Refreshed_Date],112) like @refreshed_date
GROUP BY 
	CASE WHEN [AgencyAlphaCode]='H7' THEN 'US' ELSE 'Canada' END,
	[AgencyName],	
	ISNull([MasterClientName],''),
	[AdvertiserName],
	[AdvertiserCode],
	[User],
	[Refreshed_Date],
	[BuyMonth],
	[CampaignPublicId],
	[CampaignName]

--update [OverrideDollars] column in [ALEX_RPT_PRISMA_SUMMARY_2015] table with values from [OverrideDollars] column in temp table
UPDATE [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015]
SET [OverrideDollars] = a.[OverrideDollars]
FROM temp_ALEX_RPT_PRISMA_SUMMARY_2015 a
WHERE [ALEX_RPT_PRISMA_SUMMARY_2015].[COUNTRY] = a.[COUNTRY]
AND [ALEX_RPT_PRISMA_SUMMARY_2015].[AgencyName] = a.[AgencyName]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[MasterClientName] = a.[MasterClientName]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[AdvertiserName] = a.[AdvertiserName]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[AdvertiserCode] = a.[AdvertiserCode]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[User] = a.[User]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[CampaignPublicId] = a.[CampaignPublicId]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[CampaignName] = a.[CampaignName]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[Refreshed_Date] = a.[Refreshed_Date]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[Month] = a.[BuyMonth]
and [ALEX_RPT_PRISMA_SUMMARY_2015].[RecordType] = 'O'

drop table temp_ALEX_RPT_PRISMA_SUMMARY_2015

-- insert records from [ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full] table into [ALEX_RPT_PRISMA_SUMMARY_2015] table
--applying filters [BuyType] in ('Display','Search') and PackageType in ('Child','Standalone') and [PlannedDataExists] = 1 (business rules provided by Todd Snyder)
INSERT INTO [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015](
       [COUNTRY], 
	   [AgencyName], 
	   [MasterClientName],       
	   [AdvertiserName], 
	   [AdvertiserCode],
	   [User], 
	   [CampaignPublicId],	
	   [CampaignName],
	   [Refreshed_Date], 
	   [Month], 	
	   [PlacementId], 
	   [CostMethod],
	   [# of Placements],
	   [Invalid 1X1 Placements],
	   [Placements without Delivery],
		[NA Placements],
		[Invalid1by1PlannedCost],
		[PlannedCostWithoutDelivery],
		[NAPlannedCost],
		[PlannedAmount],
		[# of Records],
	   [RecordType])
SELECT 
	[CONTRY], 
	[AgencyName], 
	UPPER([MasterClientName]), 
	[AdvertiserName], 
	[AdvertiserCode], 
	[User],
	[CampaignPublicId],
	[CampaignName],
	[Refreshed_Date], 
	[PlacementMonth], 
	[PlacementId], 
	[CostMethod], 
	1, 
	[Invalid 1x1 Placements], 
	[Placements without Delivery], 
	[NA Placements], 
	[Invalid1by1PlannedCost], 
	[PlannedCostWithoutDelivery], 
	[NAPlannedCost], 
	[PlannedAmount], 
	1, 
	'c'
FROM  [dbo].[ALEX_RPT_PRISMA_COMPLIANCE_2015_BM_Full]
WHERE 
	[BuyType] in ('Display','Search') and PackageType in ('Child','Standalone') and [PlannedDataExists] = 1 and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date


DELETE [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015]
WHERE AgencyName = 'CATALYST' and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date

DELETE [dbo].[ALEX_RPT_PRISMA_SUMMARY_2015]
WHERE MasterClientName like ('%MAZDA%') and Country = 'US' and convert(varchar(10),[Refreshed_Date],112) like @refreshed_date


--3/28 pp: added logic to delete campaignpublicids from Overrride table that are not in compliance  
delete from [ALEX_RPT_PRISMA_SUMMARY_2015]
where convert(varchar(10),[Refreshed_Date],112) like @refreshed_date and CampaignPublicId in 
( 
select distinct CampaignPublicId from  [ALEX_RPT_PRISMA_SUMMARY_2015] WHERE RecordType = 'o'
except 
select distinct CampaignPublicId from  [ALEX_RPT_PRISMA_SUMMARY_2015] WHERE RecordType = 'c'
) 



GO



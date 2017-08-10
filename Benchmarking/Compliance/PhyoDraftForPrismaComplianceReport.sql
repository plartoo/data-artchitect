USE [DM_1305_GroupMBenchmarkingUS];

DECLARE @run_id INT;
DECLARE @log_table VARCHAR(50);

DECLARE @start_date VARCHAR(50); 
DECLARE @end_date VARCHAR(50); 

-- Wipe log entries from testing stage
--  DELETE a
--  FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Log] a
--  WHERE a.PID <> 0
SET @run_id = (SELECT MAX(PID) FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Log])+1; --CONVERT(VARCHAR, GETDATE(), 112);
SET @log_table = 'Compliance_Log';
SET @start_date = '2014-01-01'; --Adjust this accordingly
SET @end_date = CONVERT(DATE, Dateadd(s, -1, Dateadd(mm, Datediff(m, 0, Getdate()), 0))); --Adjust this accordingly 


EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '1: Creating BuyOrderDetails base table'
IF OBJECT_ID('Compliance_BuyOrderDetails') IS NOT NULL
   DROP TABLE Compliance_BuyOrderDetails

SELECT
	[AgencyName]
	,[AdvertiserCode]
	,[AdvertiserName]
	,[AgencyAlphaCode]
	,[BuyAmount]
	,[BuyDetailId]
	,[BuyMfNumber]
	,[BuyMonth]
	,CONVERT(DATE, '1-'+ [BuyMonth]) AS [New_BuyMonth]
	,[BuyRcnAmount]
	,[BuyRefNumber]
	,[BuySendDate]
	,[BuySource]
	,[BuyType]
	,[CampaignCreationDate]
	,[CampaignCreationUser]
	,[CampaignEndDate]
	,[CampaignId]
	,[CampaignName]
	,[CampaignPublicId]
	,[CampaignStartDate]
	,[CampaignUser]
	,[EstimateBusinessKey]
	,[EstimateCode]
	,[IsCommissionable]
	,[IsDeleted]
	,[IsOverride]
	,[LocationCompanyCode]
	,[MediaCode]
	,[ProductBusinessKey]
	,[ProductCode]
	,[SupplierBusinessKey]
	,[SupplierCode]
	,[SupplierId]
	,[SupplierName]
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID056241_Prisma_Buy_Order_Details_Extracted]
WHERE 
	[CampaignStartDate] <= @end_date -- [campaigncreationdate] BETWEEN @start_date AND @end_date
AND [CampaignEndDate] >= @start_date


EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '2a: Creating PlacementsMonthly base table'
IF OBJECT_ID('Compliance_PlacementMonthly') IS NOT NULL
   DROP TABLE Compliance_PlacementMonthly

SELECT
[CampaignId]
,[CampaignStartDate]
,[CampaignEndDate]
,[CampaignPublicId]
,[CampaignName]
,[PackageId]
,[PackageType]
,[ParentId]
,[PlacementId]
,[PlacementMonthlyStartDate]
,[PlacementMonthlyEndDate]
--,CAST(NULL AS smalldatetime) AS [PlacementMonthlyStartDateBorrowedFromParentIfOriginallyNULL] -- copy from parent if raw monthly start date column is NULL
--,CAST(NULL AS smalldatetime) AS [PlacementMonthlyEndDateBorrowedFromParentIfOriginallyNULL] -- copy from parent if raw monthly end date column is NULL
,[PlacementStartDate]
,[PlacementEndDate]
--,[PlacementMonth] AS [PlacementMonthRaw]
--,[PlacementYear] AS [PlacementYearRaw]
,[PlacementMonth]
,[PlacementYear]
,CASE WHEN (([PlacementMonth] IS NULL) OR ([PlacementYear] IS NULL)) THEN NULL
ELSE CONVERT(SMALLDATETIME, CONCAT(CONVERT(VARCHAR, [PlacementMonth]), '-01-', CONVERT(VARCHAR, [PlacementYear])))
END AS [New_PlacementMonth] -- combine whatever raw month and year columns are into smalldatetime; if either of the columns in raw data are NULL, default to NULL
--,NULL AS [PlacementMonthBorrowedFromParentIfOriginallyNULL] -- copy from parent if raw month column is NULL
--,NULL AS [PlacementYearBorrowedFromParentIfOriginallyNULL] -- copy from parent if raw month column is NULL
--,CAST(NULL AS smalldatetime) AS [PlacementMonthYearBorrowedFromParentIfOriginallyNULL] -- combine whatever borrowed month and year columns (above) into smalldatetime; this is so that ALL child placements are filled with NOT NULL values
,[SupplierCode]
,[SupplierName]
,[AdvertiserCode]
,[AdvertiserName]
,[BuyType]
,[AdserverActions]
,[AdserverClicks]
,[AdserverCost]
,[AdserverImpressions]
,[AdserverUnits]
,[PlannedActions]
,[PlannedAmount]
,[PlannedClicks]
,[PlannedImpressions]
,[PlannedUnits]
,CAST(NULL AS float) AS [Weighted_PlannedActions]
,CAST(NULL AS float) AS [Weighted_PlannedAmount]
,CAST(NULL AS float) AS [Weighted_PlannedClicks]
,CAST(NULL AS float) AS [Weighted_PlannedImpressions]
,CAST(NULL AS float) AS [Weighted_PlannedUnits]
--,SUM(1) OVER (PARTITION BY [ParentId], [PackageType], [PlacementMonth], [PlacementYear] ) as [New_NumOfChildrenWithPlcMonthInfo] -- takes about up to 6.5 minutes total when this is added
,SUM(1) OVER (PARTITION BY [ParentId], [PackageType], [PlacementMonthlyStartDate], [PlacementMonthlyEndDate]) as [New_NumOfChildrenWithPlcMonthInfo]
,[SupplierActions]
,[SupplierClicks]
,[SupplierCost]
,[SupplierImpressions]
,[SupplierUnits]
,CASE WHEN ([PackageType] IN ('Package')) THEN 'P' ELSE 'C' END AS [RecordType]
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054941_Prisma_Placements_Monthly_Extracted]
WHERE 
	[PlacementStartDate] <= @end_date 
AND [PlacementEndDate] >= @start_date
AND [PackageType] IN ('Package', 'Child', 'Standalone')


EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '2b: Copy Planned metrics from Parents to Child'
UPDATE A
SET			
			A.[Weighted_PlannedActions] = (1.0 * B.[PlannedActions])/A.[New_NumOfChildrenWithPlcMonthInfo]
			,A.[Weighted_PlannedAmount] = (1.0 * B.[PlannedAmount])/A.[New_NumOfChildrenWithPlcMonthInfo]
			,A.[Weighted_PlannedClicks] = (1.0 * B.[PlannedClicks])/A.[New_NumOfChildrenWithPlcMonthInfo]
			,A.[Weighted_PlannedImpressions] = (1.0 * B.[PlannedImpressions])/A.[New_NumOfChildrenWithPlcMonthInfo]
			,A.[Weighted_PlannedUnits] = (1.0 * B.[PlannedUnits])/A.[New_NumOfChildrenWithPlcMonthInfo]
-- 2467172 out of 2796042 child rows affected; 326704 has PlacementMonthlyStart/EndDate NULL and therefore, they have PlannedAmt as NULL as well
FROM		(SELECT * FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] WHERE [PackageType] = 'Child') AS A
INNER JOIN	(SELECT * FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] AS C WHERE C.[PackageType] = 'Package') AS B
ON A.[ParentId] = B.[PlacementId]
-- Yufan said matching ONLY on PlacementId is enough, but Phyo thought PlacementMonthlyStartDate is necessary
--AND A.[PlacementMonthlyStartDate] = B.[PlacementMonthlyStartDate] 
--AND A.[PlacementMonthlyEndDate] = B.[PlacementMonthlyEndDate] 
-- BUT, including PlacementMonthlyStart/EndDate as JOIN key creates problem because when we SUM the original PlannedAmount from Parent ('Package') 
-- and compare that with Weighted_PlannedAmount for children ('Child') after this UPDATE statement, they differ by A LOT (like 1bn out of 10bn total).
-- Removing these two as INNER JOIN keys reduces that amount to just about 373m. Detail analysis is shared in the Benchmarking Documentation folder
-- here: V:\GRM\Dept\BusinessScience\GroupM Benchmarking\3.Documentation\Compliance Report


-- Jessie wants to treat Standalone as its own child and therefore, we need to do weighting for its own; 
-- We also decided to also copy over parent/package planned metrics from original column to weighted column
EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '2c: Copy Planned metrics from Standalone to itself for Weighted column'
UPDATE A
SET                  
                     A.[Weighted_PlannedActions] = (1.0 * A.[PlannedActions])/A.[New_NumOfChildrenWithPlcMonthInfo]
                     ,A.[Weighted_PlannedAmount] = (1.0 * A.[PlannedAmount])/A.[New_NumOfChildrenWithPlcMonthInfo]
                     ,A.[Weighted_PlannedClicks] = (1.0 * A.[PlannedClicks])/A.[New_NumOfChildrenWithPlcMonthInfo]
                     ,A.[Weighted_PlannedImpressions] = (1.0 * A.[PlannedImpressions])/A.[New_NumOfChildrenWithPlcMonthInfo]
                     ,A.[Weighted_PlannedUnits] = (1.0 * A.[PlannedUnits])/A.[New_NumOfChildrenWithPlcMonthInfo]
FROM          [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] AS A
WHERE A.[PackageType] IN ('Standalone') -- 930705 out of 930705 rows affected


EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '3: Creating PlacementDetails base table'
IF OBJECT_ID('Compliance_PlacementDetails') IS NOT NULL
   DROP TABLE Compliance_PlacementDetails

SELECT
[CampaignId]
,[CampaignPublicId]
,[CampaignName]
,[PackageId]
,[ParentId]
,[PackageType]
,[PlacementId]
,[SupplierCode]
,[SupplierName]

,[PlacementStartDate]
,[PlacementEndDate]
,[PlacementCreationDate]
,[PlacementCreationUser]
,[PlacementChangeDate]

,[CostMethod]
,[Dimension]
,[Positioning]
,[ProductCode]
,[Site]
--,[AdvertiserCode] -- cols common with BuyOrderDetails
--,[AdvertiserName]
--,[BuyType]
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails]
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054937_Prisma_Placement_Details_Extracted]
WHERE 
	[PlacementStartDate] <= @end_date 
AND [PlacementEndDate] >= @start_date
AND [PackageType] IN ('Package', 'Child', 'Standalone')


EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '4: Creating AdvPlacementDetails base table'
IF OBJECT_ID('Compliance_AdvPlacementDetails') IS NOT NULL
   DROP TABLE Compliance_AdvPlacementDetails

SELECT *
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails]
FROM 
( 
SELECT [PlacementId], [CustomColumnName], [CustomColumnValue] 
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054940_Prisma_Advanced_Placement_Details_Extracted]
) AS SourceTable
PIVOT
(
MAX([CustomColumnValue])
FOR [CustomColumnName] IN (
		[AD FORMAT FOR NON-1X1 UNITS], 
		[BUY TYPE], 
		[BUY TYPE 2], 
		[Channel Type 1], 
		[Channel Type 2], 
		[CONTENT CHANNEL DETAILS],
		[CPA KPI],
		[PRIMARY CONTENT CHANNEL],
		[RICH MEDIA TYPES - FORMAT],
		[RICH MEDIA/4TH PARTY VENDOR],
		[SECONDARY CONTENT CHANNEL],
		[Targeting Audience Type],
		[Targeting Context Type],
		[Targeting Delivery Type],
		[TARGETING TYPE 1],
		[TARGETING TYPE 2],
		[TRACKING METHOD]
	)
) AS PivotTable;


EXEC [dbo].[LogProcessNameAndLaunchTime] @log_table, @run_id, '5: Building final Placements table'
IF OBJECT_ID('Compliance_All_Placements') IS NOT NULL
   DROP TABLE Compliance_All_Placements

SELECT 
	a.[CampaignId]
	,a.[CampaignPublicId]
	,a.[CampaignName]
	,a.[CampaignStartDate]
	,a.[CampaignEndDate]
	,a.[New_NumOfChildrenWithPlcMonthInfo]
	,a.[PackageId]
	,a.[PackageType]
	,a.[ParentId]
	,a.[PlacementId]
	,a.[PlacementStartDate]
	,a.[PlacementEndDate]
	,a.[PlacementMonth]
	,a.[PlacementYear]
	,a.[New_PlacementMonth]
	,a.[PlacementMonthlyEndDate]
	,a.[PlacementMonthlyStartDate]
	,a.[BuyType]
	,a.[PlannedActions]
	,a.[PlannedAmount]
	,a.[PlannedClicks]
	,a.[PlannedImpressions]
	,a.[PlannedUnits]
	,a.[Weighted_PlannedActions]
	,a.[Weighted_PlannedAmount]
	,a.[Weighted_PlannedClicks]
	,a.[Weighted_PlannedImpressions]
	,a.[Weighted_PlannedUnits]
	,a.[AdserverActions]
	,a.[AdserverClicks]
	,a.[AdserverCost]
	,a.[AdserverImpressions]
	,a.[AdserverUnits]
	,a.[AdvertiserCode]
	,a.[AdvertiserName]
	,a.[SupplierActions]
	,a.[SupplierClicks]
	,a.[SupplierCode]
	,a.[SupplierCost]
	,a.[SupplierImpressions]
	,a.[SupplierName]
	,a.[SupplierUnits]
	,b.[CostMethod]
	,b.[Dimension]
	,b.[Positioning]
	,b.[ProductCode]
	,b.[Site]
	,c.[AD FORMAT FOR NON-1X1 UNITS]
	,c.[BUY TYPE 2]
	,c.[BUY TYPE]
	,c.[Channel Type 1]
	,c.[Channel Type 2]
	,c.[CONTENT CHANNEL DETAILS]
	,c.[CPA KPI]
	,c.[PRIMARY CONTENT CHANNEL]
	,c.[RICH MEDIA TYPES - FORMAT]
	,c.[RICH MEDIA/4TH PARTY VENDOR]
	,c.[SECONDARY CONTENT CHANNEL]
	,c.[Targeting Audience Type]
	,c.[Targeting Context Type]
	,c.[Targeting Delivery Type]
	,c.[TARGETING TYPE 1]
	,c.[TARGETING TYPE 2]
	,c.[TRACKING METHOD]
INTO [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_All_Placements] 
FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] AS a
LEFT JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails] AS b -- 340 out of 4349501 cannot me LEFT JOINED because SupplierCode is NULL
ON a.[PlacementId] = b.[PlacementId] AND a.[SupplierCode] = b.[SupplierCode]
LEFT JOIN [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails] AS c -- 4349501 rows total resulting, and takes about 16 mins to finish both LJs
ON a.[PlacementId] = c.[PlacementId]


-- returns null set
--select top 10 *
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054941_Prisma_Placements_Monthly_Extracted]
--WHERE [PlacementMonth] IS NULL AND [PackageType] <> 'Child'
--select top 10 *
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054941_Prisma_Placements_Monthly_Extracted]
--WHERE [PlacementMonth] IS NULL AND [PackageType] = 'Package'
-- proves that placementId and PlacementMonthlyStart/EndDate are primary keys
--  WITH CTE_a AS(
--
--  SELECT *
--,ROW_NUMBER() OVER(PARTITION  BY 
--  [PlacementId],
--  [PlacementMonthlyStartDate],
--  [PlacementMonthlyEndDate]
--  ORDER BY 
--  [PlacementId]
--  ) AS RN
--    FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[DFID054941_Prisma_Placements_Monthly_Extracted]
--	)
--	SELECT * FROM CTE_a WHERE RN > 1

-- How Phyo QA-ed windowing function for Weighted values
--SELECT src.*
--,SUM(1) OVER (PARTITION BY [ParentId], [PackageType], [New_PlacementMonth] ) as rowcnt
--FROM 
--(
--SELECT [CampaignId]
--      ,[ParentId]
--	  ,[PackageType]
--      ,[PlacementId]
--      ,[New_PlacementMonth]
--      ,[SupplierCode]
--      ,[PlannedAmount]
--      ,[Weighted_PlannedAmount]
--	  ,[New_NumOfChildrenWithPlcMonthInfo]
--  FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly]
--WHERE ([ParentId] =  '652253' OR [PlacementId] = '652253') AND
--[CampaignId] = '16628' AND ([New_PlacementMonth] IS NULL OR [New_PlacementMonth] = '2014-02-01 00:00:00
--) s

/*
-- how to QA distribution of parent's planned amount to children
SELECT [CampaignId]
      ,[ParentId]
	  ,[PackageType]
      ,[PlacementId]
	  ,[PlacementMonthlyStartDate]
	  ,[PlacementMonthlyEndDate]
      ,[New_PlacementMonth]
      ,[SupplierCode]
      ,[PlannedAmount]
      ,[Weighted_PlannedAmount]
	  ,[New_NumOfChildrenWithPlcMonthInfo]
  FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly]
WHERE ([ParentId] =  '652253' OR [PlacementId] = '652253') AND
[CampaignId] = '16628'
ORDER BY [PlacementMonthlyStartDate]
AND ([New_PlacementMonth] IS NULL OR [New_PlacementMonth] = '2014-02-01 00:00:00')
*/

/*
SELECT COUNT(*)
FROM Compliance_AdvPlacementDetails; -- 1,767,707

SELECT COUNT(*)
FROM Compliance_PlacementDetails; -- 1,640,317

SELECT COUNT(*)
FROM Compliance_BuyOrderDetails; -- 522,340

SELECT COUNT(*)
FROM Compliance_PlacementMonthly; --4,346,476

SELECT COUNT(DISTINCT SupplierID) -- 4124
FROM Compliance_BuyOrderDetails B

SELECT COUNT(DISTINCT SupplierCode) -- 3977
FROM Compliance_BuyOrderDetails B


-- Primary key in BuyOrderDetail table
Select *   
From (
select *,
--We don't need BuyMonth as PK
ROW_NUMBER() OVER (PARTITION BY  suppliercode, BuyRefNumber, BuyDetailId order by suppliercode) AS row_num
from  
Compliance_BuyOrderDetails
) as a
Where a.row_num > 1

-- problem here is that we have more than one IsOverride values for each CampaignId
SELECT [CampaignId], [BuyMonth], [IsOverride]
FROM Compliance_BuyOrderDetails
GROUP BY CampaignId, IsOverride, [BuyMonth]
ORDER BY 1

-- Primary key in PlacementMonthly table

Select *   
From (
select *,
ROW_NUMBER() OVER (PARTITION BY  PlacementId, New_PlacementMonth order by suppliercode) AS row_num
from  
[DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly]
) as a
Where a.row_num > 1

Select *   
From (
select *,
--We don't need BuyMonth as PK
ROW_NUMBER() OVER (PARTITION BY  PlacementId order by suppliercode) AS row_num
from  
[DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails]
) as a
Where a.row_num > 1

--[‎5/‎19/‎2017 4:28 PM] Yufan Chen: 

--but the only thing you can do 
--is 
--aggregate BOD at campaignid, suppliercode, buymonth level
--then join it to placementmonthly 
--on campaignid and suppliercode leve
--[‎5/‎19/‎2017 4:29 PM] Yufan Chen: 
--but the result only gives u a buyrcnamount at campiagn and supplier month level
----I see..does a LEFT JOIN with PlacementMonthly on the left and the aggregated BOD table on the right make sense to you? From my understanding, PlacementMonthly table has the most granular info about placements (meaning every placements in PRISMA system is there)?
--that is correct
--i use buyrcnamount as a flag 
--to find out if each placement is reconciled or not 

-- we tried joining BOD with PlacementMonthly like this

SELECT COUNT(*) -- 4,813,573
FROM Compliance_PlacementMonthly A
FULL JOIN Compliance_BuyOrderDetails B
ON A.CampaignId = B.CampaignId AND A.SupplierCode = B.SupplierCode AND A.New_PlacementMonth =  B.New_BuyMonth


SELECT COUNT(*) --4,695,484
FROM Compliance_PlacementMonthly A
LEFT JOIN Compliance_BuyOrderDetails B
ON A.CampaignId = B.CampaignId 
--AND A.[CampaignStartDate] = B.[CampaignStartDate]
--AND A.[CampaignEndDate] = B.[CampaignEndDate]
AND A.SupplierCode = B.SupplierCode 
AND A.New_PlacementMonth =  B.New_BuyMonth
AND A.BuyType = B.BuyType --4453947
*/

-- Trying to find if PlacementMonthly table covers all in PlacementDetails and AdvPlacementDetails tables
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails]
--  EXCEPT
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] -- empty result


--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] 
--EXCEPT
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails] -- empty result


--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] 
--EXCEPT
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails] -- 115237 items found


--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails] -- 240234 items found
--EXCEPT
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] 


--SELECT A.[PlacementId],[PackageType], [ParentId] -- all unmatchable rows are 'Child', 'Package' or 'Standalone'
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly]  A
--INNER JOIN (
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementMonthly] 
--EXCEPT
--SELECT DISTINCT [PlacementId]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_AdvPlacementDetails] -- 115237 items found
--) B
--ON A.PlacementId = B.PlacementId

---- Question: we have to copy over values of some columns from parent to child because some in child are missing?? (as it is done in regular Benchmarking data refresh process)
--;WITH CTE_Parent
--AS
--(
--SELECT
--[PlacementId]
--,[ParentId]
--,[PlacementStartDate]
--,[PlacementEndDate]
--,[SupplierCode]
--,[SupplierName]
--,[CostMethod]
--,[Dimension]
--,[Positioning]
--,[ProductCode]
--,[Site]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails]
--WHERE [PackageType] IN ('Package','Standalone')
--)

--UPDATE a
--SET
--a.[SupplierCode] = b.[SupplierCode]
--,a.[SupplierName] = b.[SupplierName]
--,a.[CostMethod] = b.[CostMethod]
--,a.[Dimension] = b.[Dimension]
--,a.[Positioning] = b.[Positioning]
--,a.[ProductCode] = b.[ProductCode]
--,a.[Site] = b.[Site]
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails] a
--LEFT JOIN CTE_Parent b
--ON a.[ParentId] = b.[PlacementId]
--AND a.[PlacementStartDate] = b.[PlacementStartDate]
--AND a.[PlacementEndDate] = b.[PlacementEndDate]
--WHERE a.[PackageType] = 'Child'

---- Answer: No. Because this code below shows that in Parent/Package level, there is no info about Dimension and Positioning. Not the other way around.
---- But we also have placements (both child and parent) in PlacementDetails which have no SupplierCode and SupplierName info
--SELECT top 100 [PlacementId]
--  FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails]
--WHERE [SupplierCode] IS NULL
--GROUP BY [PlacementId]

--SELECT top 100 *
--  FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_PlacementDetails]
--WHERE [SupplierCode] IS NULL AND ([PlacementId] IN ('1914096', '1914087'))

-- This is how many of the AdvPlacementDetails columns are still NULL after being LJ-ed with PlacementMonthly 
--SELECT COUNT(*) 
--FROM [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_tmp] --917,389
--WHERE [PRIMARY CONTENT CHANNEL] IS NULL -- 3,520,630
--WHERE [AD FORMAT FOR NON-1X1 UNITS] IS NULL --3,522,847
--WHERE [BUY TYPE] IS NULL --3,521,504
--WHERE [Tracking Method] is NULL --917,3

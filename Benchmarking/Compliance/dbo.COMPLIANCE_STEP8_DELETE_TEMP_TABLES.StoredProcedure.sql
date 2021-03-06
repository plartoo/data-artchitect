USE [DM_1305_GroupMBenchmarkingUS]
GO
/****** Object:  StoredProcedure [dbo].[COMPLIANCE_STEP8_DELETE_TEMP_TABLES]    Script Date: 8/10/2017 4:55:06 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =========================================================================
-- Author:		Phyo Thiha
-- Create date: 08/02/2017
-- Description:	this stored procedure deletes all the temp tables that we
-- used in the previous steps (as part of the clean up).
-- =========================================================================

CREATE PROC [dbo].[COMPLIANCE_STEP8_DELETE_TEMP_TABLES] AS
SET NOCOUNT ON;

BEGIN
IF OBJECT_ID('Compliance_Tmp_Other_Pkg_Types') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Other_Pkg_Types]

IF OBJECT_ID('Compliance_Tmp_Parent_Child_Pkg_Types') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Pkg_Types]

IF OBJECT_ID('Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Tmp_Parent_Child_Planned_Metrics_Distributed]

IF OBJECT_ID('Compliance_BuyOrderDetails') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_BuyOrderDetails]

IF OBJECT_ID('Compliance_AdvPlacementDetails') IS NOT NULL
   DROP TABLE Compliance_AdvPlacementDetails

IF OBJECT_ID('Compliance_Campaigns_Placements_Placement_Monthly') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_Placement_Monthly]

IF OBJECT_ID('Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails') IS NOT NULL
   DROP TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].[Compliance_Campaigns_Placements_PlacementMonthly_AdvPlacementDetails]
END


GO

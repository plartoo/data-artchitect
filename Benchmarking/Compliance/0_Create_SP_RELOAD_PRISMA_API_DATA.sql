

/****** Object:  StoredProcedure [dbo].[SP_RELOAD_PRISMA_API_DATA_orig]    Script Date: 6/1/2017 10:49:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




--exec [SP_RELOAD_PRISMA_API_DATA]

-- =========================================================================
-- Author:		Pavani Pagadala
-- Create date: 05/15/2015
-- Description:	this stored procedure runs all the Stored Procedures needed to refresh prisma complaince reporting Tables
-- Report requirements: US and CANADA Digital data for the 2014-2017 standard calender years
--                       
-- =========================================================================


CREATE PROC [dbo].[SP_RELOAD_PRISMA_API_DATA_orig]  
AS
-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
SET NOCOUNT ON;


BEGIN
DECLARE @ErrorMessage NVARCHAR(4000);
DECLARE @ErrorSeverity INT ;
DECLARE @ErrorState INT ;

BEGIN TRY

select getdate(),'step1'
exec [dbo].[SP_RPT_PRISMA_OVERRIDE]--00:04:06

select getdate(),'step2'
exec [dbo].[SP_RPT_PRISMA_API_CUSTOM_COLUMNS]--00:04:35

select getdate(),'step3'
exec [dbo].[SP_RPT_STG_PRISMA_COMPLIANCE]--00:11:24

select getdate(),'step4'
exec [dbo].[SP_RPT_PRISMA_COMPLIANCE]--00:06:56


-- above 4 sp's ran for 27 min on 10/30/2016
/*
select getdate(),'step5'
exec [dbo].[SP_RPT_PRISMA_OVERRIDE_BM] -- run manually after changing time stamp-- ran for 51 sec on 8/31/2016

select getdate(),'step6'
exec [dbo].[SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM] -- run manually after changing time stamp--ran for 2 min 10 sec on 8/31/2016

select getdate(),'step7'
---run this SP - this is most current 
 exec  [dbo].[SP_RPT_PRISMA_COMPLIANCE_2015_BM_UPDATE]--- run manually after running  SP_RPT_PRISMA_COMPLIANCE_CUST_COLS_BM  ---- took 1 hr 6 mins on 10/30/2016

 */

 END TRY

BEGIN CATCH
 	set @ErrorMessage  = 'Error populating table SP_RELOAD_PRISMA_API_DATA. Original error, line [' + CONVERT(VARCHAR(5), ERROR_LINE()) + ']: ' + ERROR_MESSAGE();
    set @ErrorSeverity  = ERROR_SEVERITY();
    set @ErrorState  = CASE ERROR_STATE() WHEN 0 THEN 1 ELSE ERROR_STATE() END;


    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState)
 END CATCH

END









GO



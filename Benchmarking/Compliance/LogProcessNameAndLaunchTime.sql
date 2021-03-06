USE [DM_1305_GroupMBenchmarkingUS]
GO
/****** Object:  StoredProcedure [dbo].[LogProcessNameAndLaunchTime]    Script Date: 8/2/2017 1:22:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[LogProcessNameAndLaunchTime](
  @log_table_name VARCHAR(50),
  @process_id INT,
  @process_name VARCHAR(500)
)
AS
BEGIN

	DECLARE @sql VARCHAR(1000)

	-- Make sure we create log table first
	IF OBJECT_ID(@log_table_name) IS NULL
		BEGIN
			CREATE TABLE [DM_1305_GroupMBenchmarkingUS].[dbo].table_tmp (
				PID int,
				ProcessName VARCHAR(500),
				TimeNow DATETIME,
			);
			EXEC sp_rename table_tmp, @log_table_name
		END


	IF OBJECT_ID(@log_table_name) IS NOT NULL
		BEGIN
			SET  @sql = CONCAT('INSERT INTO ', @log_table_name ,' (PID, ProcessName, TimeNow) 
						VALUES (', @process_id ,', ''', @process_name, ''', GETDATE())');
			EXEC(@sql)
		END

END


GO

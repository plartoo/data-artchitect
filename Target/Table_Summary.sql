USE [DM_1304_Target]
GO

/****** Object:  StoredProcedure [dbo].[Table_Summary]    Script Date: 10/17/2017 3:01:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[Table_Summary]  
(@tablename nvarchar(max) ='' )
AS   

	IF ISNULL(@tablename,'') = ''
	BEGIN
		SELECT 	
			t.NAME AS TableName,
			s.Name AS SchemaName,
			p.rows AS RowCounts,
			--ix.name as IndexName,
			--p.data_compression,
			SUM(a.total_pages) * 8/1024 AS TotalSpaceMB,
			(CONVERT(FLOAT,SUM(a.total_pages) * 8) /1024)/1024  AS TotalSpaceGB		
		FROM sys.tables t
			INNER JOIN sys.indexes i 
				ON t.OBJECT_ID = i.object_id
			INNER JOIN sys.partitions p 
				ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
			INNER JOIN sys.allocation_units a 
				ON p.partition_id = a.container_id
			LEFT OUTER JOIN sys.schemas s 
				ON t.schema_id = s.schema_id
			--LEFT OUTER JOIN sys.indexes IX 
			--	ON  p.object_id = ix.object_id 
			--	AND p.index_id = ix.index_id   	 	
		GROUP BY 
			t.Name, s.Name, p.Rows --,p.data_compression, ix.name
		ORDER BY 4 desc
	END
	ELSE
	BEGIN
		SELECT 	
			t.NAME AS TableName,
			s.Name AS SchemaName,
			p.rows AS RowCounts,
			--ix.name as IndexName,
			--p.data_compression,
			SUM(a.total_pages) * 8/1024 AS TotalSpaceMB,
			(CONVERT(FLOAT,SUM(a.total_pages) * 8) /1024)/1024  AS TotalSpaceGB
		FROM sys.tables t
			INNER JOIN sys.indexes i 
				ON t.OBJECT_ID = i.object_id
			INNER JOIN sys.partitions p 
				ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
			INNER JOIN sys.allocation_units a 
				ON p.partition_id = a.container_id
			LEFT OUTER JOIN sys.schemas s 
				ON t.schema_id = s.schema_id
			--LEFT OUTER JOIN sys.indexes IX 
			--	ON  p.object_id = ix.object_id 
			--	AND p.index_id = ix.index_id 
		WHERE 
			t.name IN ( SELECT Item FROM [udf_SplitString] (@tablename,','))
 	
		GROUP BY 
			t.Name, s.Name, p.Rows --,p.data_compression, ix.name
		ORDER BY 4 desc
	END


GO



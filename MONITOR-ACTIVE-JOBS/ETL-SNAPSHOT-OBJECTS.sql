/*

	ETL-SNAPSHOT-OBJECTS

*/

USE SQL_AGENT_SNAPSHOT
GO


IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'SnapshotKey'
)
BEGIN
	CREATE SEQUENCE [dbo].[SnapshotKey]  
		START WITH 1  
		INCREMENT BY 1;  
END
GO  


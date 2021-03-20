USE SQL_AGENT_SNAPSHOT
GO

EXEC dbo.SimulateQuickJobStepDuration
	@MULTIPLIER	= 1000
,	@MOD		= 666
,	@WAIT		= 0


DECLARE @SNAPSHOT_KEY BIGINT;
SELECT TOP 1 
	@SNAPSHOT_KEY = SnapshotKey
FROM [dbo].[ActiveJobsSnapshot]
ORDER BY SnapshotKey DESC;

SELECT 
	*
,	DATEDIFF(second, StartDate, EndDate) AS SecondsDuration
FROM [dbo].[ActiveJobStepHistory]
WHERE SnapshotKey = @SNAPSHOT_KEY;






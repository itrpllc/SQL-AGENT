USE SQL_AGENT_SNAPSHOT
GO

DECLARE @SNAPSHOT_KEY BIGINT;
SELECT TOP 1 
	@SNAPSHOT_KEY = SnapshotKey
FROM [dbo].[ActiveJobsSnapshot]
ORDER BY SnapshotKey DESC;

SELECT TOP 1 *
FROM dbo.ActiveJobsSnapshot
ORDER BY SnapshotKey DESC;


SELECT 
	a.SnapshotKey
,	j.JobName
,	a.StartExecutionDate
,	a.LastExecutedStepId
,	a.LastExecutedStepDate
,	a.NextScheduledRunDate
FROM dbo.ActiveJobs a
JOIN dbo.Jobs j
ON j.JobId = a.JobId
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND j.SnapshotKey = @SNAPSHOT_KEY;

SELECT 
	*
,	DATEDIFF(second, StartDate, EndDate) AS SecondsDuration
FROM [dbo].[ActiveJobStepHistory]
WHERE SnapshotKey = @SNAPSHOT_KEY
ORDER BY StartDate DESC;






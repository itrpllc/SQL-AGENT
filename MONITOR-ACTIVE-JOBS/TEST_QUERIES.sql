USE SQL_AGENT_SNAPSHOT
GO

SELECT *
FROM dbo.Jobs;


DECLARE @SNAPSHOT_KEY BIGINT;
SELECT TOP 1 
	@SNAPSHOT_KEY = SnapshotKey
FROM [dbo].[ActiveJobsSnapshot]
ORDER BY SnapshotKey DESC;

SELECT 
	a.SnapshotKey
,	a.JobKey
,	j.JobName
,	a.StartExecutionDate
,	a.LastExecutedStepId
,	a.LastExecutedStepDate
,	a.NextScheduledRunDate
,	a.JobDuration
FROM dbo.ActiveJobs a
JOIN dbo.Jobs j
ON j.JobKey = a.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY;

SELECT *
FROM dbo.ActiveJobAverageDuration
WHERE SnapshotKey = @SNAPSHOT_KEY;

SELECT *
FROM dbo.ActiveJobStepAverageDuration
WHERE SnapshotKey = @SNAPSHOT_KEY;



SELECT *
FROM dbo.ActiveJobStepHistory
WHERE SnapshotKey = @SNAPSHOT_KEY;

SELECT *
FROM dbo.ActiveJobHistory
WHERE SnapshotKey = @SNAPSHOT_KEY;

------------------------------------

SELECT TOP 1 *
FROM dbo.ActiveJobsSnapshot
ORDER BY SnapshotKey DESC;




SELECT 
	*
,	DATEDIFF(second, StartDate, EndDate) AS SecondsDuration
FROM [dbo].[ActiveJobStepHistory]
WHERE SnapshotKey = @SNAPSHOT_KEY
ORDER BY StartDate DESC;


SELECT *
FROM dbo.ActiveJobStepHistory
WHERE SnapshotKey = @SNAPSHOT_KEY;


SELECT *
FROM dbo.ActiveJobHistory
WHERE SnapshotKey = @SNAPSHOT_KEY;

/*

	TRUNCATE TABLE dbo.Jobs;

	;WITH CTE_JOB_ID AS (
		SELECT DISTINCT
			JobId
		FROM dbo.ActiveJobs
	)

	INSERT dbo.Jobs (
	 	JobId			
	,	JobName			
	,	CreatedDate		
	,	ModifiedDate	
	,	VersionNumber
	)
	SELECT
		s.job_id
	,	s.[name]
	,	s.date_created
	,	s.date_modified
	,	s.version_number
	FROM msdb.dbo.sysjobs s
	JOIN CTE_JOB_ID a
	ON a.JobId = s.job_Id
	LEFT JOIN dbo.Jobs j
	ON j.JobId = a.JobId
	WHERE j.[JobName] IS NULL;

	*/



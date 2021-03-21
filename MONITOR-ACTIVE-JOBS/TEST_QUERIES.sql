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
	'ActiveJobs'
,	a.SnapshotKey
,	a.JobKey
,	j.JobName
,	a.StartExecutionDate
,	a.LastExecutedStepId
,	a.LastExecutedStepDate
,	a.JobDuration
FROM dbo.ActiveJobs a
JOIN dbo.Jobs j
ON j.JobKey = a.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY;


-- Get the MAX step completed in the current jobs
-- and the total duration for the completed step(s)
SELECT
	'ActiveJobCompletedSteps'
,	@SNAPSHOT_KEY	AS SnapshotKey
,	a.JobKey
,	SUM(s.RunDuration)	AS StepsCompletedDuration
,	MAX(s.JobStepId)	AS LastStepCompleted
FROM dbo.ActiveJobs a
JOIN dbo.ActiveJobStepHistory s
ON a.JobKey = s.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND s.StartDate >= a.StartExecutionDate
GROUP BY a.JobKey;

SELECT 
	'ActiveJobStepAverageDuration'
,	*
FROM dbo.ActiveJobStepAverageDuration
WHERE SnapshotKey = @SNAPSHOT_KEY

-- Get the SUM of the average duration for the 
-- steps remaining
SELECT 
	'ActiveJobStepsRemaining'
,	@SNAPSHOT_KEY	AS SnapshotKey
,	a.JobKey
,	SUM(s.AverageDuration)	AS AverageRemainingDuration
FROM dbo.ActiveJobs a
JOIN dbo.ActiveJobStepAverageDuration s
ON a.JobKey = s.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND s.SnapshotKey = @SNAPSHOT_KEY
AND s.JobStepId > ISNULL(a.LastExecutedStepId, 0)
AND a.JobKey = 11
GROUP BY a.JobKey;



SELECT 
 	*
FROM dbo.ActiveJobStepHistory 
WHERE SnapshotKey = @SNAPSHOT_KEY
AND JobKey = 11










SELECT 
	'ActiveJobs'
,	a.SnapshotKey
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

SELECT 
	'ActiveJobStepHistory'
,	*
FROM dbo.ActiveJobStepHistory
WHERE SnapshotKey = @SNAPSHOT_KEY;

SELECT 
	'ActiveJobAverageDuration'
,	*
FROM dbo.ActiveJobAverageDuration
WHERE SnapshotKey = @SNAPSHOT_KEY;

SELECT 
	'ActiveJobStepAverageDuration'
,	*
FROM dbo.ActiveJobStepAverageDuration
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



DECLARE @SNAPSHOT_KEY BIGINT;
SELECT TOP 1 
	SnapshotKey
FROM [dbo].[ActiveJobsSnapshot]
ORDER BY SnapshotKey DESC;

-- 278,279,280,281

DECLARE
	@SNAPSHOT_KEY INT = 278;

SELECT 
	'ActiveJobs'
,	a.SnapshotKey
,	a.JobKey
,	j.JobName
,	a.StartExecutionDate
,	a.LastExecutedStepId
,	a.LastExecutedStepDate
,	a.JobDuration
FROM dbo.ActiveJobs a
JOIN dbo.Jobs j
ON j.JobKey = a.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND a.JobKey = 11;


-- Get the MAX step completed in the current jobs
-- and the total duration for the completed step(s)
SELECT
	'ActiveJobCompletedSteps'
,	@SNAPSHOT_KEY	AS SnapshotKey
,	a.JobKey
,	SUM(s.RunDuration)	AS StepsCompletedDuration
,	MAX(s.JobStepId)	AS LastStepCompleted
FROM dbo.ActiveJobs a
JOIN dbo.ActiveJobStepHistory s
ON a.JobKey = s.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND s.SnapshotKey = @SNAPSHOT_KEY
AND s.StartDate >= a.StartExecutionDate
AND a.JobKey = 11
GROUP BY a.JobKey;


-- Get the SUM of the average duration for the 
-- steps remaining
SELECT 
	'ActiveJobStepsRemaining'
,	@SNAPSHOT_KEY	AS SnapshotKey
,	a.JobKey
,	SUM(s.AverageDuration)	AS AverageRemainingDuration
FROM dbo.ActiveJobs a
JOIN dbo.ActiveJobStepAverageDuration s
ON a.JobKey = s.JobKey
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND s.SnapshotKey = @SNAPSHOT_KEY
AND s.JobStepId > ISNULL(a.LastExecutedStepId, 0)
AND a.JobKey = 11
GROUP BY a.JobKey;


SELECT 
	'ActiveJobStepAverageDuration'
,	*
FROM dbo.ActiveJobStepAverageDuration
WHERE SnapshotKey = @SNAPSHOT_KEY
AND JobKey = 11



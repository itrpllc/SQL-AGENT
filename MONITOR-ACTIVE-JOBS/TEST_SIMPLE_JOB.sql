/*
	Test Simple Job with 2 1 minute steps

	Start job then run SQL Agent job CAPTURE SQL AGENT JOBS RUNNING SNAPSHOT
	a couple of times to see the results.


*/

USE SQL_AGENT_SNAPSHOT
GO


DECLARE @SNAPSHOT_KEY	INT;

SELECT TOP 1 
	@SNAPSHOT_KEY = SnapshotKey
FROM [dbo].[ActiveJobsSnapshot]
ORDER BY SnapshotKey DESC;

SELECT TOP 1 *
FROM [dbo].[ActiveJobsSnapshot]
ORDER BY SnapshotKey DESC;

SELECT *
FROM dbo.ActiveJobs
WHERE SnapshotKey = @SNAPSHOT_KEY;


-------------------------------------------------------


-- Run the queries below with the following values
-- for @SNAPSHOT_KEY:
-- 282,283,284,285
--
-- View the results in the ActiveJobs table

DECLARE
	@BEGIN_SNAPSHOT_KEY INT = 282
,	@END_SNAPSHOT_KEY   INT = 285;

SELECT 
	'ActiveJobs'
,	a.SnapshotKey
,	a.JobKey
,	j.JobName
,	a.StartExecutionDate
,	a.LastExecutedStepId
,	a.LastExecutedStepDate
,	a.JobDuration
,	a.StepsCompletedDuration
,	a.LastStepCompleted
,	a.AverageRemainingDuration
,	a.EstimatedCompletionDate
FROM dbo.ActiveJobs a
JOIN dbo.Jobs j
ON j.JobKey = a.JobKey
WHERE a.SnapshotKey BETWEEN @BEGIN_SNAPSHOT_KEY AND @END_SNAPSHOT_KEY
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


/*
	Add to ActiveJobs and update with each Snapshot based on msdb.dbo.sysjobhistory"

	StepsCompletedDuration		INT -> actual duration for job steps completed
	LastStepCompleted			INT -> MAX(step_id) of steps completed so far

	AverageRemainingDuration	INT -> SUM of average duration for steps remaining

	EstimatedCompletionDate		DATETIME = 
		StartExecutionDate + 
		StepsCompletedDuration + 
		AverageRemainingStepsDuration 

*/
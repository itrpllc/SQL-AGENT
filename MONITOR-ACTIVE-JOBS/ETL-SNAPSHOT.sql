/*



*/

USE SQL_AGENT_SNAPSHOT
GO


DECLARE 
	@SNAPSHOT_KEY		BIGINT = NEXT VALUE FOR dbo.SnapshotKey
,	@SNAPSHOT_DATE		DATETIME = GETDATE()
,	@SESSION_ID			INT;

SET @SESSION_ID = (
	SELECT MAX(session_id)
	FROM msdb.dbo.syssessions
);


INSERT dbo.ActiveJobsSnapshot (
	SnapshotKey				
,	CreatedDate	
)
VALUES (
	@SNAPSHOT_KEY
,	@SNAPSHOT_DATE
);

SELECT TOP 1 *
FROM dbo.ActiveJobsSnapshot
ORDER BY SnapshotKey DESC;


-- Load jobs currently executing
INSERT dbo.ActiveJobs (
	SnapshotKey				
,	SessionId				
,	JobId					
,	RunRequestedDate		
,	RunRequestedSource		
,	QueuedDate				
,	StartExecutionDate		
,	LastExecutedStepId		
,	LastExecutedStepDate	
,	StopExecutionDate		
,	JobHistoryId			
,	NextScheduledRunDate	
)
SELECT 
	@SNAPSHOT_KEY
,	@SESSION_ID
,	job_id
,	run_requested_date
,	run_requested_source
,	queued_date
,	start_execution_date
,	last_executed_step_id
,	last_executed_step_date
,	stop_execution_date
,	job_history_id
,	next_scheduled_run_date
FROM msdb.dbo.sysjobactivity 
WHERE session_id = @SESSION_ID
AND start_execution_date IS NOT NULL
AND stop_execution_date IS NULL;


SELECT *
FROM dbo.ActiveJobs
WHERE SnapshotKey = @SNAPSHOT_KEY;


INSERT dbo.Jobs (
	SnapshotKey						
,	JobId			
,	JobName			
,	CreatedDate		
,	ModifiedDate	
,	VersionNumber
)
SELECT
	@SNAPSHOT_KEY
,	j.job_id
,	j.[name]
,	j.date_created
,	j.date_modified
,	j.version_number
FROM msdb.dbo.sysjobs j
JOIN dbo.ActiveJobs a
ON a.JobId = j.job_Id
WHERE a.SnapshotKey = @SNAPSHOT_KEY;

SELECT *
FROM dbo.Jobs
WHERE SnapshotKey = @SNAPSHOT_KEY;


-- Load all SQL Agent history for the jobs currently running
INSERT dbo.ActiveJobStepHistory (
	SnapshotKey
,	JobId			
,	JobInstanceId	
,	JobStepId		
,	JobStepName		
,	RunStatus		
,	StartDate			
,	RunDuration		
,	EndDate			
)
SELECT
	@SNAPSHOT_KEY
,	h.job_id
,	h.instance_id
,	h.step_id
,	h.step_name
,	h.run_status
,	msdb.dbo.agent_datetime(h.run_date, h.run_time) start_datetime
,	(h.[run_duration] / 10000 * 3600) +		-- convert hours to seconds
	(h.[run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
	(h.[run_duration] % 10000) % 100 / 60	AS duration_seconds
,	DATEADD(
		second
	,	h.[run_duration] / 10000 * 3600 +			-- convert hours to seconds
		(h.[run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
		(h.[run_duration] % 10000) % 100			-- get seconds
	,	msdb.dbo.agent_datetime(h.run_date, run_time)
	)
FROM dbo.ActiveJobs a
JOIN msdb.dbo.sysjobhistory h
ON h.job_id = a.JobId
WHERE a.SnapshotKey = @SNAPSHOT_KEY
AND h.run_status = 1;	-- 1=succeeded; ignore other statuses


DECLARE @TODAY DATE = GETDATE();
SELECT *
FROM dbo.ActiveJobStepHistory 
WHERE SnapshotKey = @SNAPSHOT_KEY
AND StartDate > @TODAY;







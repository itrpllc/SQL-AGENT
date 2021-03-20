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





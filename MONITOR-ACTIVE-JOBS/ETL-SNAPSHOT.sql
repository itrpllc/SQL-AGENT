/*



*/

USE SQL_AGENT_SNAPSHOT
GO

-- Get jobs currently executing
SELECT 
	start_execution_date
,	last_executed_step_id
,	last_executed_step_date
,	stop_execution_date
,	job_history_id
,	next_scheduled_run_date
,	a.job_id
,	j.[name]
FROM msdb.dbo.sysjobactivity a
JOIN msdb.dbo.sysjobs j
ON j.job_id = a.job_id
WHERE session_id = (
	SELECT MAX(session_id)
	FROM msdb.dbo.syssessions
)
AND a.start_execution_date IS NOT NULL
AND stop_execution_date IS NULL;




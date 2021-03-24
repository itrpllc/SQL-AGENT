/*

	Presentation Demo

	Demo Steps:
	1. Run CRM ETL job
	2. Run CAPTURE SQL AGENT JOBS RUNNING SNAPSHOT job
	3. Refresh Power BI report data
	4. Select the latest snapshot in the slicer
	5. Repeat steps 2 - 4

*/

USE SQL_AGENT_SNAPSHOT
GO


-- Simulate a job step that runs for a minimum
-- amount of time plus a somewhat random amount
-- of additional time
EXEC dbo.DemoJobStepDuration
	@MINIMUM_MINUTES_DURATION	 = 1
,	@WAIT						 = 0;
GO


DECLARE @ITERATOR INT = 60;
DECLARE @MSG VARCHAR(50); 
WHILE @ITERATOR > 0
BEGIN
SET @MSG = CONVERT(VARCHAR(50), @ITERATOR);
RAISERROR(@MSG, 0, 1) WITH NOWAIT;
EXEC msdb.dbo.sp_start_job 'CAPTURE SQL AGENT JOBS RUNNING SNAPSHOT'
WAITFOR DELAY '00:01';
SET @ITERATOR -= 1;
END
GO


SELECT *
FROM msdb.dbo.sysjobs
WHERE [name] = '10 Steps';

SELECT *
FROM msdb.dbo.sysjobsteps
WHERE job_id = '9163F270-031E-4A76-88E1-591A4FF737D0';


/*

	ETL-SNAPSHOT-OBJECTS

*/

USE SQL_AGENT_SNAPSHOT
GO


-- EXEC dbo.SimulateQuickJobStepDuration
CREATE OR ALTER PROCEDURE dbo.SimulateQuickJobStepDuration
	@MULTIPLIER	INT = 1000
,	@MOD		INT = 555
,	@WAIT		BIT = 1
AS
BEGIN
	DECLARE
		@SECONDS	INT
	,	@WAIT_UNTIL	DATETIME
	,	@TIME		VARCHAR(50);

	SET @SECONDS = (CAST(RAND() * @MULTIPLIER AS INT) % @MOD) + 60;
	SET @WAIT_UNTIL = DATEADD(second, @SECONDS, GETDATE());
	SET @TIME = CONVERT(VARCHAR(50), @WAIT_UNTIL, 121);
	RAISERROR(@TIME, 0, 1) WITH NOWAIT;

	IF @WAIT = 1
		WAITFOR TIME @WAIT_UNTIL;
END


CREATE OR ALTER PROCEDURE [dbo].[SimulateJobStepDuration]
	@MINIMUM_MINUTES	INT = 1
,	@MAXIMUM_MINUTES	INT = 10
,	@DEBUG				BIT = 0
AS
BEGIN
	-- convert to range of seconds
	DECLARE
	 	@MINIMUM_SECONDS		INT = @MINIMUM_MINUTES * 60
	,	@MAXIMUM_SECONDS		INT = @MAXIMUM_MINUTES * 60
	,	@STEP_DURATION_SECONDS	INT = 0
	,	@WAIT_FOR				VARCHAR(10);

	WHILE @STEP_DURATION_SECONDS < @MINIMUM_SECONDS
	BEGIN
		SET @STEP_DURATION_SECONDS = FLOOR(RAND() * @MAXIMUM_SECONDS);
	END

	DECLARE
		@HOURS		SMALLINT = 0
	,	@MINUTES 	SMALLINT = (@STEP_DURATION_SECONDS - 3600 ) / 60;

	IF @STEP_DURATION_SECONDS >= 3600
	BEGIN
		SET @HOURS = @STEP_DURATION_SECONDS / 3600;
		SET @MINUTES = (@STEP_DURATION_SECONDS - 3600 ) / 60;
	END
	ELSE
	BEGIN
		SET @MINUTES = @STEP_DURATION_SECONDS / 60;
	END

	DECLARE
		@WAITFOR_STRING		CHAR(5) = 
			RIGHT('00' + CONVERT(VARCHAR(2), @HOURS), 2) +
			':' +
			RIGHT('00' + CONVERT(VARCHAR(2), @MINUTES), 2);

	IF @DEBUG = 1
	BEGIN
		SELECT 
			@MINIMUM_MINUTES		AS [MINIMUM_MINUTES]
		,	@MAXIMUM_MINUTES		AS [MAXIMUM_MINUTES]
		,	@STEP_DURATION_SECONDS	AS [STEP_DURATION_SECONDS]
		,	@WAITFOR_STRING			AS [WAITFOR_STRING];
	END

	IF @DEBUG = 0
	BEGIN
		WAITFOR DELAY @WAITFOR_STRING;
	END
END
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


IF NOT EXISTS (
	SELECT 1
	FROM sys.tables
	WHERE name = 'ActiveJobsSnapshot'
)
CREATE TABLE dbo.ActiveJobsSnapshot (
	SnapshotKey				BIGINT
		CONSTRAINT PK_ActiveJobsSnapshot
			PRIMARY KEY CLUSTERED
,	CreatedDate				DATETIME
);


IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'Jobs'
)
CREATE TABLE dbo.Jobs (
 	JobKey			INT	IDENTITY	NOT NULL
		CONSTRAINT PK_Jobs
			PRIMARY KEY CLUSTERED
,	JobId			UNIQUEIDENTIFIER	NOT NULL
,	JobName			NVARCHAR(128)	NOT NULL
,	CreatedDate		DATETIME		NOT NULL
,	ModifiedDate	DATETIME		NULL
,	VersionNumber	INT				NOT NULL
)
GO


/*
ALTER TABLE dbo.ActiveJobs
ADD JobKey INT;

-- calulate how long the job has been running
ALTER TABLE dbo.ActiveJobs
ADD JobDuration INT;

-- Add average job duration
ALTER TABLE dbo.ActiveJobs
ADD JobAverageDuration INT;

-- Add estimated completion date based on start time + 
-- average duration of remaining steps. Account for how long
-- current step is running
ALTER TABLE dbo.ActiveJobs
ADD EstimatedCompletionDate	DATETIME;
--> 	StartExecutionDate + 
-->		StepsCompletedDuration + 
-->		AverageRemainingStepsDuration 

ALTER TABLE dbo.ActiveJobs
ADD StepsCompletedDuration	INT;
--> actual duration for job steps completed

ALTER TABLE dbo.ActiveJobs
ADD LastStepCompleted	INT;
--> MAX(step_id) of steps completed so far

ALTER TABLE dbo.ActiveJobs
ADD AverageRemainingDuration	INT; 
--> SUM of average duration for steps remaining

*/

IF NOT EXISTS (
	SELECT 1
	FROM sys.tables
	WHERE name = 'ActiveJobs'
)
CREATE TABLE dbo.ActiveJobs (
	SnapshotKey				BIGINT
,	SessionId				INT
,	JobId					UNIQUEIDENTIFIER
,	RunRequestedDate		DATETIME
,	RunRequestedSource		NVARCHAR(128)
,	QueuedDate				DATETIME
,	StartExecutionDate		DATETIME
,	LastExecutedStepId		INT
,	LastExecutedStepDate	DATETIME
,	StopExecutionDate		DATETIME
,	JobHistoryId			INT
,	NextScheduledRunDate	DATETIME
,	JobKey					INT
,	JobDuration				INT
,	JobAverageDuration		INT
,	EstimatedCompletionDate	DATETIME
,	StepsCompletedDuration	INT
,	LastStepCompleted		INT
,	AverageRemainingDuration	INT
)



IF NOT EXISTS (
	SELECT 1
	FROM sys.tables
	WHERE name = 'ExcludeJobs'
)
CREATE TABLE dbo.ExcludeJobs (
	JobId					UNIQUEIDENTIFIER
		CONSTRAINT PK_ExcludeJobs
			PRIMARY KEY CLUSTERED
);


INSERT dbo.ExcludeJobs
VALUES (
	'F22988D2-B5A2-44F6-AAE1-31C1DA6CE8C1'
);


	/*
	ALTER TABLE dbo.ActiveJobStepHistory
	ADD JobKey	INT;

	*/

IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'ActiveJobStepHistory'
)
CREATE TABLE dbo.ActiveJobStepHistory (
	SnapshotKey		INT					NOT NULL
,	JobId			UNIQUEIDENTIFIER	NOT NULL
,	JobInstanceId	INT					NOT NULL
,	JobStepId		INT					NOT NULL
,	JobStepName		NVARCHAR(128)		NOT NULL
,	RunStatus		INT					NOT NULL
,	StartDate		DATETIME			NOT NULL
,	RunDuration		INT					NOT NULL	-- seconds
,	EndDate			DATETIME			NULL
,	JobKey			INT					NOT NULL
);



IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'ActiveJobHistory'
)
CREATE TABLE dbo.ActiveJobHistory (
	SnapshotKey		INT					NOT NULL
,	JobKey			INT					NOT NULL
,	JobId			UNIQUEIDENTIFIER	NOT NULL
,	JobInstanceId	INT					NOT NULL
,	RunStatus		INT					NOT NULL
,	StartDate		DATETIME			NOT NULL
,	Duration		INT					NOT NULL	-- seconds
,	EndDate			DATETIME			NOT NULL
);



IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'ActiveJobAverageDuration'
)
CREATE TABLE dbo.ActiveJobAverageDuration (
	SnapshotKey			INT					NOT NULL
,	JobKey				INT					NOT NULL
,	JobExecutionCount	INT					NOT NULL
,	TotalDuration		INT					NOT NULL
,	AverageDuration		INT					NOT NULL
);


IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'ActiveJobStepAverageDuration'
)
CREATE TABLE dbo.ActiveJobStepAverageDuration (
	SnapshotKey			INT					NOT NULL
,	JobKey				INT					NOT NULL
,	JobStepId			INT					NOT NULL
,	JobExecutionCount	INT					NOT NULL
,	TotalDuration		INT					NOT NULL
,	AverageDuration		INT					NOT NULL
);



CREATE OR ALTER PROCEDURE dbo.CAPTURE_SQL_AGENT_JOBS_RUNNING_SNAPSHOT
AS
BEGIN
	DECLARE 
		@SNAPSHOT_KEY		BIGINT = NEXT VALUE FOR dbo.SnapshotKey
	,	@SNAPSHOT_DATE		DATETIME = GETDATE()
	,	@SESSION_ID			INT;

	-- Get the current SQL Agent session_id. Use as a filter on 
	-- msdb.dbo.sysjobactivity
	SET @SESSION_ID = (
		SELECT MAX(session_id)
		FROM msdb.dbo.syssessions
	);


	-- Insert a new row for the current snapshot. Use as filter to
	-- retrieve the data for a snapshot.
	INSERT dbo.ActiveJobsSnapshot (
		SnapshotKey				
	,	CreatedDate	
	)
	VALUES (
		@SNAPSHOT_KEY
	,	@SNAPSHOT_DATE
	);


	-- Load jobs currently running and associate with the current
	-- snapshot (@SNAPSHOT_KEY); query msdb.dbo.sysjobactivity,
	-- filter ExcludeJobs and msdb.dbo.syssessions (via @SESSION_ID)
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
	,	JobDuration
	)
	SELECT 
		@SNAPSHOT_KEY
	,	@SESSION_ID
	,	a.job_id
	,	a.run_requested_date
	,	a.run_requested_source
	,	a.queued_date
	,	a.start_execution_date
	,	a.last_executed_step_id
	,	a.last_executed_step_date
	,	a.stop_execution_date
	,	a.job_history_id
	,	a.next_scheduled_run_date
		-- calculate how long the job has been running;
		-- @SNAPSHOT_DATE is the current date time
	,	DATEDIFF(second, a.start_execution_date, @SNAPSHOT_DATE)
	FROM msdb.dbo.sysjobactivity a
	LEFT JOIN dbo.ExcludeJobs x
	ON x.JobId = a.job_id
	WHERE session_id = @SESSION_ID
	AND start_execution_date IS NOT NULL
	AND stop_execution_date IS NULL
	AND x.JobId IS NULL;


	-- Insert any JobId that isn't in the Jobs table
	-- Insert values from msdb.dbo.sysjobs
	-- Query ActiveJobs, join to msdb.dbo.sysjobs 
	-- Update ActiveJobs with JobKey (surrogate key) from Jobs
	--
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


	UPDATE a
	SET JobKey = j.JobKey
	FROM dbo.ActiveJobs a
	JOIN dbo.Jobs j
	ON j.JobId = a.JobId
	WHERE SnapshotKey = @SNAPSHOT_KEY;


	-- Load all SQL Agent job step execution history for the jobs currently 
	-- running
	-- Query msdb.dbo.sysjobhistory; filter on job step succeeded rows
	-- (1 per job step per job execution where step_id > 0)
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
	,	JobKey	
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
		,	h.[run_duration] / 10000 * 3600 +		-- convert hours to seconds
			(h.[run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
			(h.[run_duration] % 10000) % 100		-- get seconds
		,	msdb.dbo.agent_datetime(h.run_date, run_time)
		)
	,	a.JobKey
	FROM dbo.ActiveJobs a
	JOIN msdb.dbo.sysjobhistory h
	ON h.job_id = a.JobId
	WHERE a.SnapshotKey = @SNAPSHOT_KEY
	AND h.run_status = 1	-- 1=succeeded; ignore other statuses
	AND h.step_id > 0;		-- exclude the job outcome row; only get the job steps


	-- Load all SQL Agent job execution history for the jobs currently running
	-- Query msdb.dbo.sysjobhistory; filter on job outcome succeeded rows
	-- (1 per job execution, where step_id = 0)
	INSERT dbo.ActiveJobHistory (
		SnapshotKey		
	,	JobKey			
	,	JobId			
	,	JobInstanceId	
	,	RunStatus		
	,	StartDate		
	,	Duration		
	,	EndDate			
	)

	SELECT
		@SNAPSHOT_KEY
	,	a.JobKey
	,	h.job_id
	,	h.instance_id
	,	h.run_status
	,	msdb.dbo.agent_datetime(h.run_date, h.run_time) AS start_datetime
	,	(h.[run_duration] / 10000 * 3600) +		-- convert hours to seconds
		(h.[run_duration] % 10000) / 100 * 60 +	-- convert minutes to seconds
		(h.[run_duration] % 10000) % 100 / 60	AS duration_seconds
	,	DATEADD(
			second
		,	h.[run_duration] / 10000 * 3600 +			-- convert hours to seconds
			(h.[run_duration] % 10000) / 100 * 60 +		-- convert minutes to seconds
			(h.[run_duration] % 10000) % 100			-- get seconds
		,	msdb.dbo.agent_datetime(h.run_date, run_time)
		) AS end_datetime
	FROM dbo.ActiveJobs a
	JOIN msdb.dbo.sysjobhistory h
	ON h.job_id = a.JobId
	WHERE a.SnapshotKey = @SNAPSHOT_KEY
	AND h.run_status = 1	-- 1=succeeded; ignore other statuses
	AND h.step_id = 0;		-- only get the job outcome row; ignore job steps


	-- Calculate the historical average duration for the active jobs
	INSERT dbo.ActiveJobAverageDuration (
		SnapshotKey			
	,	JobKey				
	,	JobExecutionCount	
	,	TotalDuration		
	,	AverageDuration		
	)

	SELECT
		@SNAPSHOT_KEY		
	,	JobKey			
	,	COUNT(*)			
	,	SUM(Duration)		
	,	AVG(Duration)		
	FROM dbo.ActiveJobHistory 
	WHERE SnapshotKey = @SNAPSHOT_KEY
	GROUP BY JobKey;


	-- Calculate the historical average duration for the steps 
	-- in active jobs
	INSERT dbo.ActiveJobStepAverageDuration (
		SnapshotKey			
	,	JobKey				
	,	JobStepId			
	,	JobExecutionCount	
	,	TotalDuration		
	,	AverageDuration		
	)

	SELECT
		@SNAPSHOT_KEY		
	,	JobKey			
	,	JobStepId
	,	COUNT(*)			
	,	SUM(RunDuration)		
	,	AVG(RunDuration)		
	FROM dbo.ActiveJobStepHistory 
	WHERE SnapshotKey = @SNAPSHOT_KEY
	GROUP BY JobKey, JobStepId;


	-- Get the MAX step completed in the ActiveJobs
	-- and the total duration for the completed step(s)
	-- Update ActiveJobs
	;WITH CTE_ACTIVE_JOBS_COMPLETED_STEPS AS (
		SELECT
		 	a.JobKey
		,	SUM(s.RunDuration)	AS StepsCompletedDuration
		,	MAX(s.JobStepId)	AS LastStepCompleted
		FROM dbo.ActiveJobs a
		JOIN dbo.ActiveJobStepHistory s
		ON a.JobKey = s.JobKey
		WHERE a.SnapshotKey = @SNAPSHOT_KEY
		AND s.SnapshotKey = @SNAPSHOT_KEY
		AND s.StartDate >= a.StartExecutionDate
		GROUP BY a.JobKey
	)

	UPDATE a
	SET
		StepsCompletedDuration = s.StepsCompletedDuration
	,	LastStepCompleted = s.LastStepCompleted
	FROM dbo.ActiveJobs a
	JOIN CTE_ACTIVE_JOBS_COMPLETED_STEPS s
	ON s.JobKey = a.JobKey
	WHERE a.SnapshotKey = @SNAPSHOT_KEY;

	-- Get the SUM of the average duration for the 
	-- steps remaining and update ActiveJobs
	;WITH CTE_ACTIVE_JOBS_AVERAGE_REMAINING_DURATION AS (
		SELECT 
		 	a.JobKey
		,	SUM(s.AverageDuration)	AS AverageRemainingDuration
		FROM dbo.ActiveJobs a
		JOIN dbo.ActiveJobStepAverageDuration s
		ON a.JobKey = s.JobKey
		WHERE a.SnapshotKey = @SNAPSHOT_KEY
		AND s.SnapshotKey = @SNAPSHOT_KEY
		AND s.JobStepId > ISNULL(a.LastExecutedStepId, 0)
		GROUP BY a.JobKey
	)

	UPDATE a
	SET
		AverageRemainingDuration = s.AverageRemainingDuration
	,	EstimatedCompletionDate = DATEADD(
			second
		,	ISNULL(StepsCompletedDuration, JobDuration) + s.AverageRemainingDuration
		,	StartExecutionDate
	)
	FROM dbo.ActiveJobs a
	JOIN CTE_ACTIVE_JOBS_AVERAGE_REMAINING_DURATION  s
	ON s.JobKey = a.JobKey
	WHERE a.SnapshotKey = @SNAPSHOT_KEY;
END












/*

	ETL-SNAPSHOT-OBJECTS

*/

USE SQL_AGENT_SNAPSHOT
GO


-- EXEC dbo.SimulateQuickJobStepDuration
CREATE OR ALTER PROCEDURE dbo.SimulateQuickJobStepDuration
	@MULTIPLIER	INT = 1000
,	@MOD		INT = 555
AS
BEGIN
	DECLARE
		@SECONDS	INT
	,	@WAIT_UNTIL	DATETIME
	,	@TIME		VARCHAR(50);

	SET @SECONDS = CAST(RAND() * @MULTIPLIER AS INT) % @MOD;
	SET @WAIT_UNTIL = DATEADD(second, @SECONDS, GETDATE());
	SET @TIME = CONVERT(VARCHAR(50), @WAIT_UNTIL, 121);
	RAISERROR(@TIME, 0, 1) WITH NOWAIT;
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
	SnapshotKey		INT				NOT NULL
,	JobKey			INT	IDENTITY	NOT NULL
		CONSTRAINT PK_Jobs
			PRIMARY KEY CLUSTERED
,	JobId			UNIQUEIDENTIFIER	NOT NULL
,	JobName			NVARCHAR(128)	NOT NULL
,	CreatedDate		DATETIME		NOT NULL
,	ModifiedDate	DATETIME		NULL
,	VersionNumber	INT				NOT NULL
)
GO


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
)


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
);



IF NOT EXISTS (
	SELECT 1
	FROM sys.sequences
	WHERE [name] = 'JobStepHistory'
)
CREATE TABLE dbo.JobStepHistory (
	SnapshotKey		INT					NOT NULL
,	JobId			UNIQUEIDENTIFIER	NOT NULL
,	JobInstanceId	INT					NOT NULL
,	JobStepId		INT					NOT NULL
,	JobStepName		NVARCHAR(128)		NOT NULL
,	RunStatus		INT					NOT NULL
,	RunDate			DATETIME			NOT NULL
,	RunDuration		INT					NOT NULL	-- seconds
);














/*



*/


-- Get the column names and types returned from a query
declare @tsql nvarchar(max) = N'SELECT TOP 1 * FROM msdb.dbo.sysjobactivity';
select [name], system_type_name
from sys.dm_exec_describe_first_result_set(@tsql, null, 1);


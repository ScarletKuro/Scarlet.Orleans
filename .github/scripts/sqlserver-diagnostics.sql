SET NOCOUNT ON;
SELECT SYSUTCDATETIME() AS captured_utc, @@VERSION AS server_version;
SELECT sqlserver_start_time, cpu_count, physical_memory_kb FROM sys.dm_os_sys_info;
SELECT session_id, login_time, status, host_name, program_name, database_id, DB_NAME(database_id) AS database_name
FROM sys.dm_exec_sessions WHERE is_user_process=1;
SELECT CAST(t.target_data AS xml) AS system_health
FROM sys.dm_xe_session_targets t JOIN sys.dm_xe_sessions s ON s.address=t.event_session_address
WHERE s.name=N'system_health' AND t.target_name=N'ring_buffer';

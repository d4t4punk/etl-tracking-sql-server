-- Scott Newby - Steampunk
-- 2022-09-07
-- DECPREICATED - use stored procedures to execute & make your life easier!

-- SQL to create a process - your top-level ETL job/process
insert into etl_tracking.etl_process (etl_process_name, etl_process_descr) values ('Druid', 'Druid NEDS load - 2022 POC')
GO
-- Validate insert
select * from etl_tracking.etl_process 

-- SQL to create the expected steps of your ETL job/process
insert into etl_tracking.etl_exec_steps (etl_process_id,step_order_id,step_name) values ((select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = 'Druid'),1,'My first step')
GO
insert into etl_tracking.etl_exec_steps (etl_process_id,step_order_id,step_name) values ((select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = 'Druid'),2,'My second step')
GO
insert into etl_tracking.etl_exec_steps (etl_process_id,step_order_id,step_name) values ((select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = 'Druid'),3,'My third step')
GO

-- Validate insert
select * from etl_tracking.etl_exec_steps

--Create an Execution - the event of the etl process running

insert into etl_tracking.etl_executions (etl_process_id) values ((select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = 'Druid'))
GO
-- Validate insert
select * from etl_tracking.etl_executions

-- Set execution end-datetime
update etl_tracking.etl_executions set end_date_time_utc = GETUTCDATE() where etl_execution_id = 1
go
-- Validate insert
select * from etl_tracking.etl_executions

-- Create step execution tracking
-- Start a step
insert into etl_tracking.etl_execution_step_tracking (etl_execution_id,etl_exec_step_id) 
values 
(
    (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = (select max(etl_process_id) from etl_tracking.etl_process where etl_process_name = 'Druid')),
    4 -- or figure out based on step name...
)

-- Validate the execution step tracking
select * from etl_tracking.etl_execution_step_tracking

-- end a step execution
update etl_tracking.etl_execution_step_tracking set end_datetime_utc = GETUTCDATE() where exec_step_track_id = 1

-- Validate the execution step tracking
select * from etl_tracking.etl_execution_step_tracking

-- Mock a failure
insert into etl_tracking.etl_error (etl_exec_step_track_id,etl_error_short_description,etl_error_long_description) 
values 
(1, 'My error short desc', 'my error long desc')

-- Validate mock error
select * from etl_tracking.etl_error
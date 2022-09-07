-- Steampunk - ETL Tracking Database Setup
-- Scott Newby
-- 2022-08-31
-- A basic ETL tracking schema for SQL Server - track process/step executions/errors
-- 2022-09-07
-- Adding status/result code tracking, stored procedures, status and code tables along with FK and auto updates to end-datetime fields

CREATE DATABASE etl_tracking4
GO
USE etl_tracking4
GO
-- Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl_tracking')
BEGIN
    EXEC('CREATE SCHEMA [etl_tracking]')
END
GO

-- Lookup tables
-- etl_result_codes table
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_result_code]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_result_code]
GO
-- create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_result_code](
	[etl_result_code_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_result_code_name] [varchar](128) NOT NULL,
	[etl_result_code_description] [varchar](256) NULL,
	[create_datetime] [datetime] NOT NULL,
	[update_datetime] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_result_code] ADD  CONSTRAINT [PK_etl_result_code] PRIMARY KEY CLUSTERED 
(
	[etl_result_code_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_result_code] ADD  DEFAULT (getutcdate()) FOR [create_datetime]
GO

-- data
INSERT INTO [etl_tracking].[etl_result_code] ([etl_result_code_name], [etl_result_code_description]) VALUES ('Success', 'Process or Step Completed Successfully')
GO
INSERT INTO [etl_tracking].[etl_result_code] ([etl_result_code_name], [etl_result_code_description]) VALUES ('Failure', 'Process or Step Failed')
GO
INSERT INTO [etl_tracking].[etl_result_code] ([etl_result_code_name], [etl_result_code_description]) VALUES ('Success with Failures', 'Process or Step Completed but Encountered Errors')
GO
-- etl_tracking_status
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_tracking_status]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_tracking_status]
GO
-- create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_tracking_status](
	[etl_tracking_status_id] [int] IDENTITY(1,1) NOT NULL,
	[status_name] [varchar](128) NOT NULL,
	[status_description] [varchar](256) NULL,
	[create_datetime] [datetime] NOT NULL,
	[update_datetime] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_tracking_status] ADD  CONSTRAINT [PK_etl_tracking_status] PRIMARY KEY CLUSTERED 
(
	[etl_tracking_status_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_tracking_status] ADD  DEFAULT (getutcdate()) FOR [create_datetime]
GO

--data
INSERT INTO [etl_tracking].[etl_tracking_status] ([status_name],[status_description]) VALUES ('Idle', 'Process or Step is Not Running')
GO
INSERT INTO [etl_tracking].[etl_tracking_status] ([status_name],[status_description]) VALUES ('Running', 'Process or Step is Running')
GO

-- Main tables
-- etl_process table
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_process]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_process]
GO
-- create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_process](
	[etl_process_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_process_name] [varchar](128) NOT NULL,
	[etl_process_descr] [varchar](256) NULL,
	[create_datetime_utc] [datetime] NOT NULL,
	[update_datetime_utc] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_process] ADD  CONSTRAINT [PK_etl_process] PRIMARY KEY CLUSTERED 
(
	[etl_process_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_process] ADD  DEFAULT (getutcdate()) FOR [create_datetime_utc]
GO

-- etl_exec_steps
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_exec_steps]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_exec_steps]
GO
-- create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_exec_steps](
	[etl_exec_step_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_process_id] [int] NOT NULL,
	[step_order_id] [int] NOT NULL,
	[step_name] [varchar](128) NOT NULL,
	[create_datetime_utc] [datetime] NOT NULL,
	[update_datetime_utc] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_exec_steps] ADD  CONSTRAINT [PK_etl_exec_steps] PRIMARY KEY CLUSTERED 
(
	[etl_exec_step_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_exec_steps] ADD  DEFAULT (getutcdate()) FOR [create_datetime_utc]
GO
ALTER TABLE [etl_tracking].[etl_exec_steps]  WITH CHECK ADD  CONSTRAINT [FK_etl_process_step_id] FOREIGN KEY([etl_process_id])
REFERENCES [etl_tracking].[etl_process] ([etl_process_id])
GO
ALTER TABLE [etl_tracking].[etl_exec_steps] CHECK CONSTRAINT [FK_etl_process_step_id]
GO

-- etl_executions
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_executions]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_executions]
GO
-- create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_executions](
	[etl_execution_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_process_id] [int] NOT NULL,
	[etl_tracking_status_id] [int] NOT NULL,
	[etl_result_code_id] [int] NOT NULL,
	[start_datetime_utc] [datetime] NOT NULL,
	[end_datetime_utc] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_executions] ADD  CONSTRAINT [PK_etl_executions] PRIMARY KEY CLUSTERED 
(
	[etl_execution_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_executions] ADD  DEFAULT (getutcdate()) FOR [start_datetime_utc]
GO
ALTER TABLE [etl_tracking].[etl_executions]  WITH CHECK ADD  CONSTRAINT [FK_etl_process_id] FOREIGN KEY([etl_process_id])
REFERENCES [etl_tracking].[etl_process] ([etl_process_id])
GO
ALTER TABLE [etl_tracking].[etl_executions] CHECK CONSTRAINT [FK_etl_process_id]
GO
ALTER TABLE [etl_tracking].[etl_executions]  WITH CHECK ADD  CONSTRAINT [FK_etl_result_code_ex_id] FOREIGN KEY([etl_result_code_id])
REFERENCES [etl_tracking].[etl_result_code] ([etl_result_code_id])
GO
ALTER TABLE [etl_tracking].[etl_executions] CHECK CONSTRAINT [FK_etl_result_code_ex_id]
GO
ALTER TABLE [etl_tracking].[etl_executions]  WITH CHECK ADD  CONSTRAINT [FK_etl_tracking_status_ex_id] FOREIGN KEY([etl_tracking_status_id])
REFERENCES [etl_tracking].[etl_tracking_status] ([etl_tracking_status_id])
GO
ALTER TABLE [etl_tracking].[etl_executions] CHECK CONSTRAINT [FK_etl_tracking_status_ex_id]
GO

-- etl_execution_step_tracking
--drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_execution_step_tracking]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_execution_step_tracking]
GO
--create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_execution_step_tracking](
	[exec_step_track_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_execution_id] [int] NOT NULL,
	[etl_exec_step_id] [int] NOT NULL,
	[etl_exec_rows_processed] [bigint] NOT NULL,
	[etl_tracking_status_id] [int] NOT NULL,
	[etl_result_code_id] [int] NOT NULL,
	[start_datetime_utc] [datetime] NOT NULL,
	[end_datetime_utc] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] ADD  CONSTRAINT [PK_etl_execution_step_tracking] PRIMARY KEY CLUSTERED 
(
	[exec_step_track_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] ADD  DEFAULT ((0)) FOR [etl_exec_rows_processed]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] ADD  DEFAULT (getutcdate()) FOR [start_datetime_utc]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking]  WITH CHECK ADD  CONSTRAINT [FK_etl_exec_step_st_id] FOREIGN KEY([etl_exec_step_id])
REFERENCES [etl_tracking].[etl_exec_steps] ([etl_exec_step_id])
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] CHECK CONSTRAINT [FK_etl_exec_step_st_id]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking]  WITH CHECK ADD  CONSTRAINT [FK_etl_execution_st_id] FOREIGN KEY([etl_execution_id])
REFERENCES [etl_tracking].[etl_executions] ([etl_execution_id])
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] CHECK CONSTRAINT [FK_etl_execution_st_id]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking]  WITH NOCHECK ADD  CONSTRAINT [FK_etl_result_code_st_id] FOREIGN KEY([etl_result_code_id])
REFERENCES [etl_tracking].[etl_result_code] ([etl_result_code_id])
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] CHECK CONSTRAINT [FK_etl_result_code_st_id]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking]  WITH CHECK ADD  CONSTRAINT [FK_etl_tracking_status_st_id] FOREIGN KEY([etl_tracking_status_id])
REFERENCES [etl_tracking].[etl_tracking_status] ([etl_tracking_status_id])
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] CHECK CONSTRAINT [FK_etl_tracking_status_st_id]
GO

-- etl_error
--drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_error]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_error]
GO
--create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_error](
	[etl_error_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_exec_step_track_id] [int] NOT NULL,
	[etl_error_short_description] [varchar](256) NOT NULL,
	[etl_error_long_description] [varchar](1024) NOT NULL,
	[etl_error_create_date_utc] [datetime] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_error] ADD  CONSTRAINT [PK_etl_error] PRIMARY KEY CLUSTERED 
(
	[etl_error_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_error] ADD  DEFAULT (getutcdate()) FOR [etl_error_create_date_utc]
GO
ALTER TABLE [etl_tracking].[etl_error]  WITH CHECK ADD  CONSTRAINT [FK_etl_execution_error_st_id] FOREIGN KEY([etl_exec_step_track_id])
REFERENCES [etl_tracking].[etl_execution_step_tracking] ([exec_step_track_id])
GO
ALTER TABLE [etl_tracking].[etl_error] CHECK CONSTRAINT [FK_etl_execution_error_st_id]
GO

-- stored procedures
DROP PROCEDURE [etl_tracking].[stpc_ins_etl_exec_step_tracking]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_ins_etl_exec_step_tracking] @etl_process_name varchar(128), @step_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution (event of etl kicking off) record - to end the execution event
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
DECLARE @etl_exec_step_id int
BEGIN TRY
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    SET @etl_exec_step_id = (select top 1 etl_exec_step_id from etl_tracking.etl_exec_steps where step_name = @step_name)
    insert into  etl_tracking.etl_execution_step_tracking 
        (etl_execution_id, etl_exec_step_id) 
    values
        (@etl_execution_id, @etl_exec_step_id)
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_ins_etl_exec_step_tracking_failure]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_ins_etl_exec_step_tracking_failure] @etl_process_name varchar(128), @step_name varchar(128), @etl_error_short_desc varchar(256), @etl_error_long_descr varchar(1024) 
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution (event of etl kicking off) record - to end the execution event
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
DECLARE @etl_exec_step_id int
DECLARE @etl_exec_step_track_id int
BEGIN TRY
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    SET @etl_exec_step_id = (select top 1 etl_exec_step_id from etl_tracking.etl_exec_steps where step_name = @step_name)
    SET @etl_exec_step_track_id = (select max(exec_step_track_id) from etl_tracking.etl_execution_step_tracking where etl_exec_step_id = @etl_exec_step_id)
    insert into etl_tracking.etl_error 
        (etl_exec_step_track_id,etl_error_short_description,etl_error_long_description) 
    values 
        (@etl_exec_step_track_id, @etl_error_short_desc, @etl_error_long_descr)
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_ins_etl_execution]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_ins_etl_execution] @etl_process_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to create an etl execution (event of etl kicking off)record
DECLARE @etl_process_id int
BEGIN TRY
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    insert into etl_tracking.etl_executions
        (etl_process_id) 
    values 
        (@etl_process_id)
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_ins_etl_process]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_ins_etl_process] @etl_proc_name varchar(128), @etl_proc_descr varchar(256)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to create an etl process record
BEGIN TRY
    insert into etl_tracking.etl_process 
        (etl_process_name, etl_process_descr) 
    values 
        (@etl_proc_name, @etl_proc_descr)
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_ins_etl_step]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_ins_etl_step] @etl_process_name varchar(128), @step_order_id int, @step_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to create an etl step (child of process) record
DECLARE @etl_process_id int
BEGIN TRY
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    insert into etl_tracking.etl_exec_steps 
        (etl_process_id,step_order_id,step_name) 
    values 
        (@etl_process_id, @step_order_id, @step_name)
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_upd_etl_execution_end]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_upd_etl_execution_end] @etl_process_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution (event of etl kicking off) record - to end the execution event
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
BEGIN TRY
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    update etl_tracking.etl_executions 
        set end_datetime_utc = GETUTCDATE() 
    where etl_execution_id = @etl_execution_id
    
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_upd_execution_result]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_upd_execution_result] @etl_process_name varchar(128), @etl_result_code_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution result status
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
DECLARE @etl_result_code_id int
BEGIN TRY
    -- set/lookup variables
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    SET @etl_result_code_id = (select etl_result_code_id from etl_tracking.etl_result_code where etl_result_code_name = @etl_result_code_name)
    -- the result is indicative of ending the process execution
    update etl_tracking.etl_executions set etl_result_code_id = @etl_result_code_id, end_datetime_utc = GETUTCDATE()
    where etl_execution_id = @etl_execution_id
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO

DROP PROCEDURE [etl_tracking].[stpc_upd_execution_status]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_upd_execution_status] @etl_process_name varchar(128), @etl_result_status_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution result status
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
DECLARE @etl_result_status_id int
BEGIN TRY
    -- set/lookup variables
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    SET @etl_result_status_id = (select etl_tracking_status_id from etl_tracking.etl_tracking_status where status_name = @etl_result_status_name)
    -- the result is indicative of ending the process execution
    update etl_tracking.etl_executions set etl_tracking_status_id = @etl_result_status_id
    where etl_execution_id = @etl_execution_id
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_upd_execution_step_result]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_upd_execution_step_result] @etl_process_name varchar(128), @etl_exec_step_name varchar(128), @etl_result_code_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution step tracking result code
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
DECLARE @etl_exec_step_id int
DECLARE @etl_execution_step_track_id int
DECLARE @etl_result_code_id int
BEGIN TRY
    -- set/lookup variables
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    SET @etl_execution_step_track_id = (select exec_step_track_id from etl_tracking.etl_execution_step_tracking where etl_execution_id = @etl_execution_id and etl_exec_step_id = @etl_exec_step_id)
    SET @etl_exec_step_id = (select etl_exec_step_id from etl_tracking.etl_exec_steps where etl_process_id = @etl_process_id and step_name = @etl_exec_step_name)
    SET @etl_result_code_id = (select etl_result_code_id from etl_tracking.etl_result_code where etl_result_code_name = @etl_result_code_name)
    -- the result is indicative of ending the process execution
    update etl_tracking.etl_execution_step_tracking set etl_result_code_id = @etl_result_code_id, end_datetime_utc = GETUTCDATE()
    where etl_execution_id = @etl_execution_id and etl_exec_step_id = @etl_exec_step_id
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[stpc_upd_execution_step_status]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[stpc_upd_execution_step_status] @etl_process_name varchar(128), @etl_exec_step_name varchar(128), @etl_result_status_name varchar(128)
AS
-- 2022-09-06
-- Scott Newby (Steampunk)
-- Description:
-- Stored proc to update an etl execution step tracking status code
DECLARE @etl_process_id int
DECLARE @etl_execution_id int
DECLARE @etl_exec_step_id int
DECLARE @etl_execution_step_track_id int
DECLARE @etl_result_status_id int
BEGIN TRY
    -- set/lookup variables
    SET @etl_process_id = (select top 1 etl_process_id from etl_tracking.etl_process where etl_process_name = @etl_process_name)
    SET @etl_execution_id = (select max(etl_execution_id) from etl_tracking.etl_executions where etl_process_id = @etl_process_id)
    SET @etl_execution_step_track_id = (select exec_step_track_id from etl_tracking.etl_execution_step_tracking where etl_execution_id = @etl_execution_id and etl_exec_step_id = @etl_exec_step_id)
    SET @etl_exec_step_id = (select etl_exec_step_id from etl_tracking.etl_exec_steps where etl_process_id = @etl_process_id and step_name = @etl_exec_step_name)
    SET @etl_result_status_id = (select etl_tracking_status_id from etl_tracking.etl_tracking_status where status_name = @etl_result_status_name)
    -- the result is indicative of ending the process execution
    update etl_tracking.etl_execution_step_tracking set etl_tracking_status_id = @etl_result_status_id
    where etl_execution_id = @etl_execution_id and etl_exec_step_id = @etl_exec_step_id
END TRY
BEGIN CATCH
    EXECUTE etl_tracking.usp_GetErrorInfo; 
END CATCH;


GO


DROP PROCEDURE [etl_tracking].[usp_GetErrorInfo]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etl_tracking].[usp_GetErrorInfo]  
AS  
SELECT  
    ERROR_NUMBER() AS ErrorNumber  
    ,ERROR_SEVERITY() AS ErrorSeverity  
    ,ERROR_STATE() AS ErrorState  
    ,ERROR_PROCEDURE() AS ErrorProcedure  
    ,ERROR_LINE() AS ErrorLine  
    ,ERROR_MESSAGE() AS ErrorMessage;  
GO

-- Steampunk - ETL Tracking Database Setup
-- Scott Newby
-- 2022-08-31
-- A basic ETL tracking schema for SQL Server - track process/step executions/errors

--CREATE DATABASE etl_tracking2
-- ALTER DATABASE [etl_tracking] SET OFFLINE WITH ROLLBACK IMMEDIATE;
-- DROP DATABASE etl_tracking;

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl_tracking')
BEGIN
    EXEC('CREATE SCHEMA [etl_tracking]')
END
GO

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

-- etl_execution_step_tracking
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_execution_step_tracking]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_execution_step_tracking]
GO
-- create
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl_tracking].[etl_execution_step_tracking](
	[exec_step_track_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_execution_id] [int] NOT NULL,
	[etl_exec_step_id] [int] NOT NULL,
	[start_datetime_utc] [datetime] NOT NULL,
	[end_datetime_utc] [datetime] NULL
) ON [PRIMARY]
GO
ALTER TABLE [etl_tracking].[etl_execution_step_tracking] ADD  CONSTRAINT [PK_etl_execution_step_tracking] PRIMARY KEY CLUSTERED 
(
	[exec_step_track_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
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

-- etl_error
-- drop
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl_tracking].[etl_error]') AND type in (N'U'))
DROP TABLE [etl_tracking].[etl_error]
GO
-- create
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

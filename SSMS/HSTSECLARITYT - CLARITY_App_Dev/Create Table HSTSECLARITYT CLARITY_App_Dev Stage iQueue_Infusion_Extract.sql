USE [CLARITY_App_Dev]
GO

-- ===========================================
-- Create table Stage.iQueue_Infusion_Extract
-- ===========================================
IF EXISTS (SELECT TABLE_NAME 
	       FROM   INFORMATION_SCHEMA.TABLES
	       WHERE  TABLE_SCHEMA = N'Stage' AND
	              TABLE_NAME = N'iQueue_Infusion_Extract')
   DROP TABLE [Stage].[iQueue_Infusion_Extract]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Stage].[iQueue_Infusion_Extract](
	[Appointment ID] [NUMERIC](18, 0) NOT NULL,
	[PAT_MRN_ID] [VARCHAR](256) NULL,
	[Unit Name] [VARCHAR](254) NULL,
	[Visit Type] [VARCHAR](50) NULL,
	[Appointment Type] [VARCHAR](200) NULL,
	[Expected Duration] [INT] NULL,
	[Appointment Status] [VARCHAR](20) NULL,
	[Appointment Time] [DATETIME] NULL,
	[Check-in Time] [DATETIME] NULL,
	[Chair Time] [DATETIME] NULL,
	[First Med Start] [DATETIME] NULL,
	[Last Med Stop] [DATETIME] NULL,
	[BCN INFUSION NURSE ASSIGNMENT] [DATETIME] NULL,
	[T UVA AMB PATIENT UNDERSTANDING AVS] [DATETIME] NULL,
	[Appointment Made Date] [DATETIME] NULL,
	[Cancel Date] [DATETIME] NULL,
	[Linked Appointment Flag] [VARCHAR](1) NULL,
	[Clinic Appointment Time] [DATETIME] NULL,
	[Clinic Appointment Length] [INT] NULL,
	[Treatment Plan] [VARCHAR](1200) NULL,
	[Treatment Plan Provider] [VARCHAR](1200) NULL,
	[Intake Time] [DATETIME] NULL,
	[Check-out Time] [DATETIME] NULL,
	[UPDATE_DATE] [DATETIME] NULL,
	[ETL_guid] [VARCHAR](50) NULL,
	[Load_Dte] [SMALLDATETIME] NULL,
 CONSTRAINT [PK_iQueue_Infusion_Extract] PRIMARY KEY CLUSTERED 
(
	[Appointment ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO



USE [CLARITY_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =====================================================================================
-- Create procedure uspSrc_iQueue_Infusion_Center_Daily
-- =====================================================================================
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.ROUTINES 
    WHERE ROUTINE_SCHEMA='Rptg'
    AND ROUTINE_TYPE='PROCEDURE'
    AND ROUTINE_NAME='uspSrc_iQueue_Infusion_Center_Daily')
   DROP PROCEDURE Rptg.[uspSrc_iQueue_Infusion_Center_Daily]
GO

CREATE PROCEDURE [Rptg].[uspSrc_iQueue_Infusion_Center_Daily]
       (
        @StartDate SMALLDATETIME = NULL
       ,@EndDate SMALLDATETIME = NULL)
AS
/****************************************************************************************************************************************
WHAT: Create procedure Rptg.uspSrc_iQueue_Infusion_Center_Daily
WHO : Tom Burgan
WHEN: 10/18/2017
WHY : Daily feed of infusion data for Cancer Center iQueue project
----------------------------------------------------------------------------------------------------------------------------------------
INFO:
      INPUTS:   dbo.V_SCHED_APPT
	            dbo.PAT_ENC
				dbo.CLARITY_DEP
				dbo.MAR_ADMIN_INFO
				dbo.ZC_MAR_RSLT
				dbo.ZC_MED_DURATION_UN
				dbo.ZC_MED_UNIT
				dbo.ZC_MAR_RSN
				dbo.PAT_ENC
				dbo.CLARITY_EMP
				dbo.CLARITY_DEP
				dbo.ORDER_MED
				dbo.CLARITY_MEDICATION
				dbo.ZC_INFUSION_TYPE
				dbo.ZC_MAR_ADMIN_TYPE
				dbo.PAT_ENC_HSP
				dbo.PATIENT
				dbo.IP_FLWSHT_REC
				dbo.IP_FLWSHT_MEAS
				dbo.IP_FLO_GP_DATA
				dbo.IP_FLT_DATA

  Temp tables :
                #InfusionPatient
                #ScheduledAppointment
                #ScheduledAppointmentPlus
                #ScheduledAppointmentLinked
                #ScheduledInfusionAppointment
                #MAR
				#TreatmentPlan
				#MARplus
                #Completed
                #NewBag
                #OrderMed
                #OrderMedSummary
                #FLT
                #FLM
                #FLTPIVOT
                #FLMPIVOT
                #ScheduledInfusionAppointmentDetail
                #RptgTemp

      OUTPUTS:
                CLARITY_App.Stage.iQueue_Infusion_Extract
----------------------------------------------------------------------------------------------------------------------------------------
MODS:     10/18/2017--TMB-- Create new stored procedure
          11/02/2017--wdr4f-Bill Reed - Modified to create table from the temp tables
		  01/22/2018--TMB-- Add flowsheet timestamp for measure BCN INFUSION NURSE ASSIGNMENT to table; change column name for
		                    "Disposition Time"
		  04/11/2018--TMB-- Add treatment plan name and treatment plan order provider
		  06/25/2018--TMB-- Add T UVA PATIENT TRACKING timestamps for measures B - Intake and G- Checked Out
		  10/16/2019--TMB-- Add hashed PAT_MRN_ID and UPDATE_DATE
*****************************************************************************************************************************************/

  SET NOCOUNT ON;

---------------------------------------------------
---Default date range is the prior day up to the following two months
  DECLARE @CurrDate SMALLDATETIME;

  SET @CurrDate = CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME);

  IF @StartDate IS NULL
      BEGIN
          -- Yesterday's date
          SET @StartDate = CAST(CAST(DATEADD(DAY, -1, @CurrDate) AS DATE) AS SMALLDATETIME)
          + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME);
      END;
  IF @EndDate IS NULL
      BEGIN
          -- End of month, two months ahead from current date
          SET @EndDate = CAST(EOMONTH(@CurrDate, 2) AS SMALLDATETIME)
          + CAST(CAST('23:59:59' AS TIME) AS SMALLDATETIME);
      END;
----------------------------------------------------

  -- Create temp table #InfusionPatient with PAT_ID and Appt date

  SELECT DISTINCT
     pa.PAT_ID
    ,CAST(pa.[APPT_DTTM] AS DATE) AS [Appt date]
	,pt.PAT_MRN_ID
  INTO #InfusionPatient
  FROM [CLARITY].[dbo].[V_SCHED_APPT] AS pa
  LEFT OUTER JOIN [CLARITY].[dbo].[PATIENT] AS pt
  ON pt.PAT_ID = pa.PAT_ID
  WHERE
  (pa.[APPT_DTTM] >= @StartDate AND pa.[APPT_DTTM] <= @EndDate)
   AND (pa.DEPARTMENT_ID IN (10210004))

  -- Create index for temp table #InfusionPatient

  CREATE UNIQUE CLUSTERED INDEX IX_InfusionPatient ON #InfusionPatient ([PAT_ID], [Appt date])

  -- Create temp table #ScheduledAppointment

  SELECT
     pa.PAT_ENC_CSN_ID                               AS [Appointment ID]
	,HASHBYTES('SHA2_256',CAST(inf.PAT_MRN_ID AS VARCHAR(10))) AS [PAT_MRN_ID]
    ,dep.DEPARTMENT_NAME                             AS [Unit Name]
    ,pa.DEPT_SPECIALTY_NAME                          AS [Visit Type]
    ,pa.PRC_NAME                                     AS [Appointment Type]
    ,pa.APPT_LENGTH                                  AS [Expected Duration]
    ,CASE
        WHEN pa.APPT_STATUS_C = 1 THEN 'sch'
        WHEN pa.APPT_STATUS_C = 2 THEN 'comp'
        WHEN pa.APPT_STATUS_C = 3 THEN 'canc'
        WHEN pa.APPT_STATUS_C = 4 THEN 'no show'
        WHEN pa.APPT_STATUS_C = 5 THEN 'left'
        WHEN pa.APPT_STATUS_C = 6 THEN 'arrived'
     END                                             AS [Appointment Status]
    ,CAST(pa.[APPT_DTTM]           AS DATE)          AS [Appointment Date]
    ,CAST(pa.[APPT_DTTM]           AS SMALLDATETIME) AS [Appointment Time]
    ,CAST(pa.CHECKIN_DTTM          AS SMALLDATETIME) AS [Check-in Time]
    ,CAST(pa.ARVL_LIST_REMOVE_DTTM AS SMALLDATETIME) AS [Chair Time]
    ,CAST(pa.APPT_MADE_DATE        AS DATE)          AS [Appointment Made Date]
    ,CAST(pa.APPT_CANC_DATE        AS DATE)          AS [Cancel Date]
    ,LAG(pa.[APPT_STATUS_C]) OVER (PARTITION BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE) ORDER BY pa.[APPT_DTTM], pa.PAT_ENC_CSN_ID) AS [Previous APPT_STATUS_C]
    ,pa.PAT_ID
    ,enc.INPATIENT_DATA_ID
    ,pa.DEPARTMENT_ID
    ,ROW_NUMBER() OVER (PARTITION BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE) ORDER BY pa.[APPT_DTTM]) AS SeqNbr
    ,ROW_NUMBER() OVER (ORDER BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE), pa.[APPT_DTTM]) AS RecordId
	,fpa.UPDATE_DATE
  INTO #ScheduledAppointment
  FROM [CLARITY].[dbo].[V_SCHED_APPT]     AS pa
  INNER JOIN #InfusionPatient             AS inf   ON pa.PAT_ID          = inf.PAT_ID
    AND CAST(pa.[APPT_DTTM] AS DATE) = inf.[Appt Date]
  LEFT OUTER JOIN [CLARITY].[dbo].[F_SCHED_APPT]	AS fpa	ON fpa.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.PAT_ENC     AS enc   ON enc.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS dep   ON pa.DEPARTMENT_ID   = dep.DEPARTMENT_ID

  -- Create index for temp table #ScheduledAppointment

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointment ON #ScheduledAppointment ([PAT_ID], [Appointment Date], RecordId)

  -- Create temp table #ScheduledAppointmentPlus

  SELECT
     pa.[Appointment ID]
	,pa.PAT_MRN_ID
    ,pa.[Unit Name]
    ,pa.[Visit Type]
    ,pa.[Appointment Type]
    ,pa.[Expected Duration]
    ,pa.[Appointment Status]
    ,pa.[Appointment Date]
    ,pa.[Appointment Time]
    ,pa.SeqNbr
    ,pa.RecordId
    ,CASE WHEN pa.[Previous APPT_STATUS_C] IN (3,4,5) THEN
           (SELECT MAX(sa.RecordId) FROM #ScheduledAppointment AS sa
                WHERE sa.RecordId < pa.RecordId
                AND sa.PAT_ID = pa.PAT_ID
                AND sa.[Appointment Date] = pa.[Appointment Date]
                AND sa.[Appointment Status] IN ('sch','comp','arrived'))
          ELSE pa.RecordId - 1
     END                                AS [Previous RecordId]
    ,pa.[Check-in Time]
    ,pa.[Chair Time]
    ,pa.[Appointment Made Date]
    ,pa.[Cancel Date]
    ,pa.[Previous APPT_STATUS_C]
    ,pa.PAT_ID
    ,pa.INPATIENT_DATA_ID
    ,pa.DEPARTMENT_ID
	,pa.UPDATE_DATE
  INTO #ScheduledAppointmentPlus
  FROM #ScheduledAppointment     AS pa

  -- Create index for temp table #ScheduledAppointmentPlus

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentPlus ON #ScheduledAppointmentPlus ([PAT_ID], [Appointment Date], [RecordId], [Previous RecordId])

  -- Create temp table #ScheduledAppointmentLinked

  SELECT
     apptplus.[Appointment ID]
	,apptplus.PAT_MRN_ID
    ,apptplus.[Unit Name]
    ,apptplus.[Visit Type]
    ,apptplus.[Appointment Type]
    ,apptplus.[Expected Duration]
    ,apptplus.[Appointment Status]
    ,apptplus.[Appointment Date]
    ,apptplus.[Appointment Time]
    ,apptplus.[Check-in Time]
    ,apptplus.[Chair Time]
    ,apptplus.[Appointment Made Date]
    ,apptplus.[Cancel Date]
    ,CASE WHEN ((apptplus.SeqNbr = 1) OR ((apptplus.SeqNbr > 1) AND (apptplus.[Previous RecordId] IS NULL))) THEN NULL
          ELSE appt.[Appointment ID]
     END AS [Previous Appointment ID]
    ,CASE WHEN ((apptplus.SeqNbr = 1) OR ((apptplus.SeqNbr > 1) AND (apptplus.[Previous RecordId] IS NULL))) THEN NULL
          ELSE appt.[Appointment Time]
     END AS [Previous Appointment Time]
    ,CASE WHEN ((apptplus.SeqNbr = 1) OR ((apptplus.SeqNbr > 1) AND (apptplus.[Previous RecordId] IS NULL))) THEN NULL
          ELSE appt.[Expected Duration]
     END AS [Previous Expected Duration]
    ,apptplus.PAT_ID
    ,apptplus.INPATIENT_DATA_ID
    ,apptplus.DEPARTMENT_ID
	,apptplus.UPDATE_DATE
  INTO #ScheduledAppointmentLinked
  FROM #ScheduledAppointmentPlus        AS apptplus
  LEFT OUTER JOIN #ScheduledAppointment AS appt ON appt.RecordId = apptplus.[Previous RecordId]

  -- Create index for temp table #ScheduledAppointmentLinked

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentLinked ON #ScheduledAppointmentLinked ([PAT_ID], [Appointment ID], [Appointment Time])

  -- Create temp table #ScheduledInfusionAppointment

  SELECT
     pa.[Appointment ID]
	,pa.PAT_MRN_ID
    ,pa.[Unit Name]
    ,pa.[Visit Type]
    ,pa.[Appointment Type]
    ,pa.[Expected Duration]
    ,pa.[Appointment Status]
    ,pa.[Appointment Date]
    ,pa.[Appointment Time]
    ,pa.[Check-in Time]
    ,pa.[Chair Time]
    ,pa.[Appointment Made Date]
    ,pa.[Cancel Date]
    ,pa.[Previous Appointment ID]
    ,pa.[Previous Appointment Time]
    ,pa.[Previous Expected Duration]
    ,pa.PAT_ID
    ,pa.INPATIENT_DATA_ID
    ,pa.DEPARTMENT_ID
	,pa.UPDATE_DATE
  INTO #ScheduledInfusionAppointment
  FROM #ScheduledAppointmentLinked AS pa
  WHERE pa.DEPARTMENT_ID IN (10210004)

  -- Create index for temp table #ScheduledInfusionAppointment

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledInfusionAppointment ON #ScheduledInfusionAppointment ([PAT_ID], [Appointment ID], [Appointment Time])

  -- Create temp table #MAR

  SELECT
    PAT_ENC.DEPARTMENT_ID
   ,CLARITY_DEP.DEPARTMENT_NAME
   ,MAR_ADMIN_INFO.MAR_ENC_CSN
   ,MAR_ADMIN_INFO.ORDER_MED_ID
   ,MAR_ADMIN_INFO.LINE
   ,MAR_ADMIN_INFO.EDITED_LINE
   ,MAR_ADMIN_INFO.SCHEDULED_TIME
   ,CONVERT(VARCHAR(10),MAR_ADMIN_INFO.SCHEDULED_TIME,101) AS MAR_Date
   --, MAR_ADMIN_INFO.MAR_SCHD_DTTM
   ,MAR_ADMIN_INFO.TAKEN_TIME
   --, LEAD(MAR_ADMIN_INFO.TAKEN_TIME) OVER (PARTITION BY MAR_ADMIN_INFO.ORDER_MED_ID ORDER BY MAR_ADMIN_INFO.TAKEN_TIME) AS next_TAKEN_TIME
   ,CONVERT(VARCHAR(8),MAR_ADMIN_INFO.TAKEN_TIME,108) AS HHMMSS
   ,MAR_ADMIN_INFO.MAR_ACTION_C
   --, LEAD(MAR_ADMIN_INFO.MAR_ACTION_C) OVER (PARTITION BY MAR_ADMIN_INFO.ORDER_MED_ID ORDER BY MAR_ADMIN_INFO.TAKEN_TIME) AS next_MAR_ACTION_C
   ,ZC_MAR_RSLT.NAME AS MAR_ACTION_NAME
   ,MAR_ADMIN_INFO.INFUSION_RATE
   ,MAR_ADMIN_INFO.MAR_DURATION
   ,ZC_MED_DURATION_UN.NAME AS MAR_DURATION_NAME
   ,CASE
       WHEN MAR_ADMIN_INFO.DUE_ACTION_C IS NULL AND ZC_MAR_RSLT.NAME = 'Taken'  THEN 'Taken: '
       WHEN MAR_ADMIN_INFO.DUE_ACTION_C IS NULL AND ZC_MAR_RSLT.NAME = 'Missed' THEN 'Missed: '
       WHEN MAR_ADMIN_INFO.DUE_ACTION_C IS NULL                                 THEN ZC_MAR_RSLT.NAME
       WHEN MAR_ADMIN_INFO.DUE_ACTION_C = 1                                     THEN 'Due'
     END AS MAR_Status
   ,MAR_ADMIN_INFO.SIG                AS Dose
   ,ZC_MED_UNIT.NAME                  AS DOSE_UNIT_NAME
   ,MAR_ADMIN_INFO.USER_ID
   ,CASE
       WHEN MAR_ADMIN_INFO.USER_ID IS NULL THEN ''
       WHEN CLARITY_EMP.USER_ID IS NULL THEN 'Unknown: ' + MAR_ADMIN_INFO.USER_ID
       WHEN CLARITY_EMP.NAME IS NULL THEN 'No Name: ' + CLARITY_EMP.USER_ID
       ELSE CLARITY_EMP.NAME + ' - ' + CLARITY_EMP.USER_ID
     END AS MAR_User
   ,MAR_ADMIN_INFO.COMMENTS           AS MAR_Comments
   ,ORDER_MED.MEDICATION_ID
   ,CLARITY_MEDICATION.NAME
   ,PAT_ENC.PAT_ID
   ,PAT_ENC.INPATIENT_DATA_ID
   ,PATIENT.PAT_NAME
   ,ORDER_MEDINFO.MAR_ADMIN_TYPE_C
   ,ZC_MAR_ADMIN_TYPE.NAME            AS MAR_ADMIN_TYPE_NAME
   --, ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID, MAR_ADMIN_INFO.MAR_ENC_CSN ORDER BY MAR_ADMIN_INFO.TAKEN_TIME) AS SeqNbr
  INTO #MAR
  FROM CLARITY.dbo.MAR_ADMIN_INFO                AS MAR_ADMIN_INFO
  LEFT OUTER JOIN CLARITY.dbo.ZC_MAR_RSLT        AS ZC_MAR_RSLT        ON MAR_ADMIN_INFO.MAR_ACTION_C        = ZC_MAR_RSLT.RESULT_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_MED_DURATION_UN AS ZC_MED_DURATION_UN ON MAR_ADMIN_INFO.MAR_DURATION_UNIT_C = ZC_MED_DURATION_UN.MED_DURATION_UN_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_MED_UNIT        AS ZC_MED_UNIT        ON MAR_ADMIN_INFO.DOSE_UNIT_C         = ZC_MED_UNIT.DISP_QTYUNIT_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_MAR_RSN         AS ZC_MAR_RSN         ON MAR_ADMIN_INFO.REASON_C            = ZC_MAR_RSN.REASON_C
  LEFT OUTER JOIN CLARITY.dbo.PAT_ENC            AS PAT_ENC            ON MAR_ADMIN_INFO.MAR_ENC_CSN         = PAT_ENC.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP        AS CLARITY_EMP        ON MAR_ADMIN_INFO.USER_ID             = CLARITY_EMP.USER_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP        AS CLARITY_DEP        ON PAT_ENC.DEPARTMENT_ID              = CLARITY_DEP.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.ORDER_MED          AS ORDER_MED          ON MAR_ADMIN_INFO.ORDER_MED_ID        = ORDER_MED.ORDER_MED_ID
  LEFT OUTER JOIN CLARITY.dbo.ORDER_MEDINFO      AS ORDER_MEDINFO      ON MAR_ADMIN_INFO.ORDER_MED_ID        = ORDER_MEDINFO.ORDER_MED_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_MEDICATION AS CLARITY_MEDICATION ON ORDER_MED.MEDICATION_ID            = CLARITY_MEDICATION.MEDICATION_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_INFUSION_TYPE   AS ZC_INFUSION_TYPE   ON ORDER_MEDINFO.INFUSION_TYPE_C      = ZC_INFUSION_TYPE.INFUSION_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_MAR_ADMIN_TYPE  AS ZC_MAR_ADMIN_TYPE  ON ORDER_MEDINFO.MAR_ADMIN_TYPE_C     = ZC_MAR_ADMIN_TYPE.MAR_ADMIN_TYPE_C
--LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_HSP        AS PAT_ENC_HSP        ON PAT_ENC.PAT_ENC_CSN_ID             = PAT_ENC_HSP.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.PATIENT            AS PATIENT            ON PAT_ENC.PAT_ID                     = PATIENT.PAT_ID
  WHERE MAR_ADMIN_INFO.SCHEDULED_TIME BETWEEN @StartDate AND @EndDate
    AND ORDER_MEDINFO.MAR_ADMIN_TYPE_C = 1                             -- Infusion
    --AND ZC_MAR_RSLT.NAME = 'New Bag'
    --AND MAR_ADMIN_INFO.INFUSION_RATE IS NOT NULL
    AND MAR_ADMIN_INFO.MAR_ACTION_C <> 100                             -- Due
    AND PAT_ENC.DEPARTMENT_ID = 10210004

  -- Create index for temp table #MAR

  CREATE UNIQUE CLUSTERED INDEX IX_MAR ON #MAR ([ORDER_MED_ID], [TAKEN_TIME])

  -- Create temp table #TreatmentPlan

  SELECT
    mar.ORDER_MED_ID
   ,ONC_TREATMENT_PLAN_ORDERS.TREATMENT_PLAN_ID
   ,ONC_TREATMENT_PLAN_ORDERS.PLAN_NAME
   ,ONC_TREATMENT_PLAN_ORDERS.PLAN_PROV_ID
   ,CLARITY_SER.PROV_NAME AS PLAN_PROV_NAME
  INTO #TreatmentPlan
  FROM (SELECT DISTINCT
			ORDER_MED_ID
	    FROM #MAR) mar
  INNER JOIN CLARITY.dbo.V_ONC_TREATMENT_PLAN_ORDERS AS ONC_TREATMENT_PLAN_ORDERS ON ONC_TREATMENT_PLAN_ORDERS.ORDER_ID = mar.ORDER_MED_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS CLARITY_SER ON ONC_TREATMENT_PLAN_ORDERS.PLAN_PROV_ID = CLARITY_SER.PROV_ID

  -- Create index for temp table #TreatmentPlan

  CREATE UNIQUE CLUSTERED INDEX IX_TreatmentPlan ON #TreatmentPlan ([ORDER_MED_ID])

  -- Create temp table #MARplus

  SELECT
    mar.*
   ,tpl.TREATMENT_PLAN_ID
   ,tpl.PLAN_NAME
   ,tpl.PLAN_PROV_ID
   ,tpl.PLAN_PROV_NAME
  INTO #MARplus
  FROM #MAR mar
  LEFT OUTER JOIN #TreatmentPlan tpl on tpl.ORDER_MED_ID = mar.ORDER_MED_ID

  -- Create index for temp table #MARplus

  CREATE UNIQUE CLUSTERED INDEX IX_MARplus ON #MARplus ([MAR_ENC_CSN], [ORDER_MED_ID], [TAKEN_TIME])

  -- Create temp table #Completed

  SELECT MAR_ENC_CSN
     , ORDER_MED_ID
     , MAX(TAKEN_TIME) AS TAKEN_TIME
  INTO #Completed
  FROM #MARplus
  --WHERE MAR_ACTION_C = 120                                           -- Completed
  WHERE MAR_ACTION_C IN (8,120)                                        -- Stopped, Completed
  GROUP BY MAR_ENC_CSN
         , ORDER_MED_ID

  -- Create index for temp table #Completed

  CREATE UNIQUE CLUSTERED INDEX IX_Completed ON #Completed ([MAR_ENC_CSN], [ORDER_MED_ID], [TAKEN_TIME])

  -- Create temp table #NewBag

  SELECT
     MAR_ENC_CSN
    ,ORDER_MED_ID
    ,MIN(TAKEN_TIME) AS TAKEN_TIME
  INTO #NewBag
  FROM #MARplus
  WHERE MAR_ACTION_C = 6                                               -- New Bag
  GROUP BY MAR_ENC_CSN
         , ORDER_MED_ID

  -- Create index for temp table #NewBag

  CREATE UNIQUE CLUSTERED INDEX IX_NewBag ON #NewBag ([MAR_ENC_CSN], [ORDER_MED_ID], [TAKEN_TIME])

  -- Create temp table #OrderMed

  SELECT
         mar.PAT_ID
       , mar.MAR_ENC_CSN
       , mar.ORDER_MED_ID
	   , mar.TREATMENT_PLAN_ID
	   , mar.PLAN_NAME
	   , mar.PLAN_PROV_NAME
       , mar.MAR_Date
       , mar.MAR_ACTION_C
       , mar.MAR_ACTION_NAME
       , mar.TAKEN_TIME
       , mar.HHMMSS
       , LEAD(mar.TAKEN_TIME) OVER (PARTITION BY mar.MAR_ENC_CSN, mar.ORDER_MED_ID ORDER BY mar.TAKEN_TIME) AS next_TAKEN_TIME
       , LEAD(mar.HHMMSS)     OVER (PARTITION BY mar.MAR_ENC_CSN, mar.ORDER_MED_ID ORDER BY mar.TAKEN_TIME) AS next_HHMMSS
       , ROW_NUMBER()         OVER (PARTITION BY mar.MAR_ENC_CSN, mar.ORDER_MED_ID ORDER BY mar.TAKEN_TIME) AS SeqNbr
  INTO #OrderMed
  FROM #MARplus mar
  INNER JOIN (SELECT
                 newbag.MAR_ENC_CSN
                ,newbag.ORDER_MED_ID
                ,newbag.TAKEN_TIME
              FROM #NewBag               AS newbag
              UNION ALL
              SELECT
                 completed.MAR_ENC_CSN
                ,completed.ORDER_MED_ID
                ,completed.TAKEN_TIME
              FROM #Completed completed) AS infusion
    ON   mar.MAR_ENC_CSN  = infusion.MAR_ENC_CSN
     AND mar.ORDER_MED_ID = infusion.ORDER_MED_ID
     AND mar.TAKEN_TIME   = infusion.TAKEN_TIME

  -- Create index for temp table #OrderMed

  CREATE UNIQUE CLUSTERED INDEX IX_OrderMed ON #OrderMed ([MAR_ENC_CSN], [ORDER_MED_ID], [TAKEN_TIME])

  -- Create temp table #OrderMedSummary

  SELECT PAT_ID
       , MAR_ENC_CSN
	   , (SELECT COALESCE(MAX(omt.PLAN_NAME),'')  + '|' AS [text()]
		  FROM #OrderMed omt
		  WHERE omt.PAT_ID = om.PAT_ID
		  AND omt.MAR_ENC_CSN = om.MAR_ENC_CSN
		  GROUP BY omt.TREATMENT_PLAN_ID
		         , omt.PLAN_NAME
	      FOR XML PATH (''), TYPE
	     ) AS PLAN_NAME
	   , (SELECT COALESCE(MAX(omt.PLAN_PROV_NAME),'')  + '|' AS [text()]
		  FROM #OrderMed omt
		  WHERE omt.PAT_ID = om.PAT_ID
		  AND omt.MAR_ENC_CSN = om.MAR_ENC_CSN
		  GROUP BY omt.TREATMENT_PLAN_ID
		         , omt.PLAN_PROV_NAME
	      FOR XML PATH ('')
	     ) AS PLAN_PROV_NAME
	   , COUNT(DISTINCT om.TREATMENT_PLAN_ID) AS PLAN_COUNT
       , MIN(TAKEN_TIME)      AS START_TAKEN_TIME
       , MAX(next_TAKEN_TIME) AS STOP_TAKEN_TIME
  INTO #OrderMedSummary
  FROM #OrderMed om
  WHERE SeqNbr = 1 AND next_TAKEN_TIME IS NOT NULL
  GROUP BY PAT_ID
         , MAR_ENC_CSN

  -- Create index for temp table #OrderMedSummary

  CREATE UNIQUE CLUSTERED INDEX IX_OrderMedSummary ON #OrderMedSummary ([MAR_ENC_CSN])

  -- Create temp table #FLT

  SELECT DISTINCT
     appt.[Appointment ID]
    ,appt.PAT_ID
    ,appt.INPATIENT_DATA_ID
    ,flt.RECORDED_TIME
	,flt.FLT_ID
    ,flt.FLO_MEAS_ID
	,flt.TEMPLATE_NAME
    ,flt.MEAS_VALUE
    ,ROW_NUMBER() OVER (PARTITION BY appt.PAT_ID, appt.[Appointment ID], flt.FLT_ID ORDER BY flt.RECORDED_TIME) AS SeqNbr
  INTO #FLT
  FROM (SELECT DISTINCT
               [Appointment ID]
             , PAT_ID
             , INPATIENT_DATA_ID
        FROM #ScheduledInfusionAppointment) appt
  INNER JOIN (SELECT
                 fr.PAT_ID
                ,fm.RECORDED_TIME
                ,fm.MEAS_VALUE
                ,fm.FLT_ID
                ,fm.FLO_MEAS_ID
	            ,ft.TEMPLATE_NAME
                ,fr.INPATIENT_DATA_ID
            FROM CLARITY.dbo.IP_FLWSHT_REC        AS fr WITH (NOLOCK)
            INNER JOIN CLARITY.dbo.IP_FLWSHT_MEAS AS fm WITH (NOLOCK) ON fr.FSD_ID      = fm.FSD_ID
            INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA AS gp WITH (NOLOCK) ON fm.FLO_MEAS_ID = gp.FLO_MEAS_ID
		    LEFT OUTER JOIN CLARITY.dbo.IP_FLT_DATA ft WITH (NOLOCK) ON ft.TEMPLATE_ID = fm.FLT_ID
            WHERE
			      (fm.FLO_MEAS_ID IN ('2103800002','2103800003') -- T UVA AMB PATIENT UNDERSTANDING AVS
                   AND fm.MEAS_VALUE = 'Yes')
				  OR fm.FLT_ID IN ('1150000005' -- BCN INFUSION NURSE ASSIGNMENT
								  )
             ) flt
  ON appt.INPATIENT_DATA_ID = flt.INPATIENT_DATA_ID

  -- Create index for temp table #FLT

  CREATE UNIQUE CLUSTERED INDEX IX_FLT ON #FLT ([Appointment ID], FLT_ID, SeqNbr)

  -- Create temp table #FLM

  SELECT DISTINCT
     appt.[Appointment ID]
    ,appt.PAT_ID
    ,appt.INPATIENT_DATA_ID
    ,flm.RECORDED_TIME
    ,flm.FLO_MEAS_ID
    ,flm.MEAS_VALUE
    ,ROW_NUMBER() OVER (PARTITION BY appt.PAT_ID, appt.[Appointment ID], flm.FLO_MEAS_ID, flm.MEAS_VALUE ORDER BY flm.RECORDED_TIME) AS SeqNbrAsc
    ,ROW_NUMBER() OVER (PARTITION BY appt.PAT_ID, appt.[Appointment ID], flm.FLO_MEAS_ID, flm.MEAS_VALUE ORDER BY flm.RECORDED_TIME DESC) AS SeqNbrDesc
	,COUNT(flm.MEAS_VALUE) over(partition by appt.PAT_ID, appt.[Appointment ID]) as Meas_Count
	,COUNT(*) over(partition by appt.PAT_ID, appt.[Appointment ID], flm.MEAS_VALUE) as Meas_Value_Count
  INTO #FLM
  FROM (SELECT DISTINCT
               [Appointment ID]
             , PAT_ID
             , INPATIENT_DATA_ID
        FROM #ScheduledInfusionAppointment) appt
  INNER JOIN (SELECT
                 fr.PAT_ID
                ,fm.RECORDED_TIME
                ,fm.MEAS_VALUE
                ,fm.FLO_MEAS_ID
                ,fr.INPATIENT_DATA_ID
            FROM CLARITY.dbo.IP_FLWSHT_REC        AS fr WITH (NOLOCK)
            INNER JOIN CLARITY.dbo.IP_FLWSHT_MEAS AS fm WITH (NOLOCK) ON fr.FSD_ID      = fm.FSD_ID
            INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA AS gp WITH (NOLOCK) ON fm.FLO_MEAS_ID = gp.FLO_MEAS_ID
            WHERE
			      (fm.FLO_MEAS_ID IN ('4147') -- T UVA PATIENT TRACKING
                      AND fm.MEAS_VALUE IN ('B - Intake','G- Checked Out'))
             ) flm
  ON appt.INPATIENT_DATA_ID = flm.INPATIENT_DATA_ID
  ORDER BY appt.[Appointment ID], flm.FLO_MEAS_ID, flm.MEAS_VALUE, flm.RECORDED_TIME

  -- Create index for temp table #FLM

  --CREATE UNIQUE CLUSTERED INDEX IX_FLM ON #FLM ([Appointment ID], FLO_MEAS_ID, MEAS_VALUE, RECORDED_TIME)
  --CREATE NONCLUSTERED INDEX IX_FLM ON #FLM ([Appointment ID], FLO_MEAS_ID, MEAS_VALUE, RECORDED_TIME)

  -- Create temp table #FLTPIVOT

  SELECT
     PAT_ID
   , [Appointment ID]
   , [1150000005] AS [BCN INFUSION NURSE ASSIGNMENT]
   , [2103800001] AS [T UVA AMB PATIENT UNDERSTANDING AVS]
  INTO #FLTPIVOT
  FROM
  (SELECT PAT_ID
        , [Appointment ID]
        , FLT_ID
		, RECORDED_TIME
   FROM #FLT
   WHERE SeqNbr = 1) FlwSht
  PIVOT
  (
  MAX(RECORDED_TIME)
  FOR FLT_ID IN ([1150000005] -- BCN INFUSION NURSE ASSIGNMENT
			   , [2103800001] -- T UVA AMB PATIENT UNDERSTANDING AVS
			    )
  ) AS PivotTable

  -- Create index for temp table #FLTPIVOT

  CREATE UNIQUE CLUSTERED INDEX IX_FLTPIVOT ON #FLTPIVOT ([Appointment ID])

  -- Create temp table #FLMPIVOT

  SELECT
     PAT_ID
   , [Appointment ID]
   , [B - Intake] AS [B - Intake]
   , [G- Checked Out] AS [G- Checked Out]
  INTO #FLMPIVOT
  FROM
  (SELECT PAT_ID
        , [Appointment ID]
        , MEAS_VALUE
		, RECORDED_TIME
   FROM #FLM
   WHERE MEAS_VALUE = 'B - Intake'
   AND SeqNbrAsc = 1
   UNION ALL
   SELECT PAT_ID
        , [Appointment ID]
        , MEAS_VALUE
		, RECORDED_TIME
   FROM #FLM
   WHERE MEAS_VALUE = 'G- Checked Out'
   AND SeqNbrDesc = 1
   ) FlwSht
  PIVOT
  (
  MAX(RECORDED_TIME)
  FOR MEAS_VALUE IN ([B - Intake] -- B - Intake
			   , [G- Checked Out] -- G- Checked Out
			    )
  ) AS PivotTable

  -- Create index for temp table #FLMPIVOT

  CREATE UNIQUE CLUSTERED INDEX IX_FLMPIVOT ON #FLMPIVOT ([Appointment ID])

  -- Create temp table #ScheduledInfusionAppointmentDetail

  SELECT
     pa.PAT_ID
    ,pa.[Appointment ID]
	,pa.PAT_MRN_ID
    ,pa.[Unit Name]
    ,pa.[Visit Type]
    ,pa.[Appointment Type]
    ,pa.[Expected Duration]
    ,pa.[Appointment Status]
    ,pa.[Appointment Date]
    ,pa.[Appointment Time]
    ,pa.[Check-in Time]
    ,CAST(flm.[B - Intake] AS SMALLDATETIME) AS [Intake Time]
    ,CAST(flm.[G- Checked Out] AS SMALLDATETIME) AS [Check-out Time]
    ,pa.[Chair Time]
	,om.PLAN_NAME AS [Treatment Plan]
	,om.PLAN_PROV_NAME AS [Treatment Plan Provider]
    ,CAST(om.START_TAKEN_TIME AS SMALLDATETIME) AS [First Med Start]
    ,CAST(om.STOP_TAKEN_TIME  AS SMALLDATETIME) AS [Last Med Stop]
    ,CAST(flt.[BCN INFUSION NURSE ASSIGNMENT] AS SMALLDATETIME) AS [BCN INFUSION NURSE ASSIGNMENT]
    ,CAST(flt.[T UVA AMB PATIENT UNDERSTANDING AVS] AS SMALLDATETIME) AS [T UVA AMB PATIENT UNDERSTANDING AVS]
    ,pa.[Appointment Made Date]
    ,pa.[Cancel Date]
    ,pa.[Previous Appointment ID]
    ,pa.[Previous Appointment Time]
    ,pa.[Previous Expected Duration]
	,pa.UPDATE_DATE
  INTO #ScheduledInfusionAppointmentDetail
  FROM #ScheduledInfusionAppointment AS pa
  LEFT OUTER JOIN (SELECT MAR_ENC_CSN
	                    , oms.PLAN_NAME.value('(./text())[1]','VARCHAR(MAX)') AS PLAN_NAME
	                    , PLAN_PROV_NAME
                        , START_TAKEN_TIME
	                    , STOP_TAKEN_TIME
                   FROM #OrderMedSummary oms) om
  ON om.MAR_ENC_CSN = pa.[Appointment ID]
  LEFT OUTER JOIN (SELECT [Appointment ID]
                        , [BCN INFUSION NURSE ASSIGNMENT]
                        , [T UVA AMB PATIENT UNDERSTANDING AVS]
                   FROM #FLTPIVOT) flt
  ON flt.[Appointment ID] = pa.[Appointment ID]
  LEFT OUTER JOIN (SELECT [Appointment ID]
                        , [B - Intake]
                        , [G- Checked Out]
                   FROM #FLMPIVOT) flm
  ON flm.[Appointment ID] = pa.[Appointment ID]

  -- Create temp table #RptgTemp

  SELECT
     A.[Appointment ID]
	,A.[PAT_MRN_ID]
    ,A.[Unit Name]
    ,A.[Visit Type]
    ,A.[Appointment Type]
    ,A.[Expected Duration]
    ,A.[Appointment Status]
    ,A.[Appointment Time]
    ,A.[Check-in Time]
	,A.[Intake Time]
	,A.[Check-out Time]
    ,A.[Chair Time]
    ,A.[First Med Start]
    ,A.[Last Med Stop]
    ,A.[BCN INFUSION NURSE ASSIGNMENT]
    ,A.[T UVA AMB PATIENT UNDERSTANDING AVS]
    ,A.[Appointment Made Date]
    ,A.[Cancel Date]
    ,A.[Linked Appointment Flag]
    ,A.[Clinic Appointment Time]
    ,A.[Clinic Appointment Length]
	,A.[Treatment Plan]
	,A.[Treatment Plan Provider]
	,A.[UPDATE_DATE]
    ,'Rptg.uspSrc_iQueue_Infusion_Center_Daily' AS [ETL_guid]
    ,GETDATE()                                  AS Load_Dte
  INTO #RptgTemp FROM
   (
    SELECT
       [Appointment ID]
	  ,[PAT_MRN_ID]
      ,[Unit Name]
      ,[Visit Type]
      ,[Appointment Type]
      ,[Expected Duration]
      ,[Appointment Status]
      ,[Appointment Time]
      ,[Check-in Time]
	  ,[Intake Time]
	  ,[Check-out Time]
      ,[Chair Time]
      ,[First Med Start]
      ,[Last Med Stop]
      ,[BCN INFUSION NURSE ASSIGNMENT]
      ,[T UVA AMB PATIENT UNDERSTANDING AVS]
      ,[Appointment Made Date]
      ,[Cancel Date]
      ,CASE WHEN [Previous Appointment ID] IS NULL THEN 'N' ELSE 'Y' END AS [Linked Appointment Flag]
      ,[Previous Appointment Time]         AS [Clinic Appointment Time]
      ,[Previous Expected Duration]        AS [Clinic Appointment Length]
	  ,[Treatment Plan]
	  ,[Treatment Plan Provider]
	  ,UPDATE_DATE
    FROM #ScheduledInfusionAppointmentDetail AS pa
   ) A

  -- Put contents of temp table #RptgTemp into db table

  INSERT INTO CLARITY_App_Dev.Stage.iQueue_Infusion_Extract
              ([Appointment ID],[PAT_MRN_ID],[Unit Name],[Visit Type],[Appointment Type],[Expected Duration],[Appointment Status],
               [Appointment Time],[Check-in Time],[Chair Time],[First Med Start],[Last Med Stop],
               [BCN INFUSION NURSE ASSIGNMENT],[T UVA AMB PATIENT UNDERSTANDING AVS],[Appointment Made Date],[Cancel Date],[Linked Appointment Flag],
               [Clinic Appointment Time],[Clinic Appointment Length],[Treatment Plan],[Treatment Plan Provider],
			   [Intake Time],[Check-out Time],[UPDATE_DATE],[ETL_guid],[Load_Dte])

  SELECT
    [Appointment ID]
   ,ISNULL(CONVERT(VARCHAR(256),[PAT_MRN_ID],2),'')					AS [PAT_MRN_ID]
   ,[Unit Name]
   ,[Visit Type]
   ,[Appointment Type]
   ,[Expected Duration]
   ,[Appointment Status]
   ,[Appointment Time]
   ,[Check-in Time]
   ,[Chair Time]
   ,[First Med Start]
   ,[Last Med Stop]
   ,[BCN INFUSION NURSE ASSIGNMENT]
   ,[T UVA AMB PATIENT UNDERSTANDING AVS]
   ,[Appointment Made Date]
   ,[Cancel Date]
   ,[Linked Appointment Flag]
   ,[Clinic Appointment Time]
   ,[Clinic Appointment Length]
   ,SUBSTRING(ISNULL(RTRIM([Treatment Plan]),'|'),1,200)         AS [Treatment Plan]
   ,SUBSTRING(ISNULL(RTRIM([Treatment Plan Provider]),'|'),1,18) AS [Treatment Plan Provider]
   ,[Intake Time]
   ,[Check-out Time]
   ,[UPDATE_DATE]
   ,[ETL_guid]
   ,[Load_Dte]
  FROM #RptgTemp
  ORDER BY [Appointment Time]

GO



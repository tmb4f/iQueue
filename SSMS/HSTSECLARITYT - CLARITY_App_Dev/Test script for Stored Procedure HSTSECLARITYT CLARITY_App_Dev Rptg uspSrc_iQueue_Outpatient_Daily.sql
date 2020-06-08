USE [CLARITY_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME = NULL
       ,@EndDate SMALLDATETIME = NULL

--SET @StartDate = '4/5/2018 00:00 AM'
--SET @EndDate = '7/1/2019 11:59 PM'
SET @StartDate = '1/1/2018 00:00 AM'
SET @EndDate = '2/25/2020 11:59 PM'
--SET @EndDate = '4/6/2018 11:59 PM'
--SET @StartDate = '5/11/2019 00:00 AM'
--SET @EndDate = '5/13/2019 11:59 PM'

--ALTER PROCEDURE [Rptg].[uspSrc_iQueue_Outpatient_Daily]
--       (
--        @StartDate SMALLDATETIME = NULL
--       ,@EndDate SMALLDATETIME = NULL)
--AS
/****************************************************************************************************************************************
WHAT: Create procedure Rptg.uspSrc_iQueue_Outpatient_Daily
WHO : Tom Burgan
WHEN: 04/17/2019
WHY : Daily feed of patient flow data for Ambulatory Optimization iQueue project
----------------------------------------------------------------------------------------------------------------------------------------
INFO:
      INPUTS:   dbo.V_SCHED_APPT
	            dbo.PATIENT
				dbo.PAT_ENC_RSN_VISIT
				dbo.CL_RSN_FOR_VISIT
				dbo.PAT_ENC_APPT_NOTES
				dbo.F_SCHED_APPT
				dbo.PAT_ENC
				dbo.CLARITY_DEP
				dbo.CLARITY_SER
				dbo.REFERRAL
				dbo.REFERRAL_HIST
                dbo.ZC_APPT_STATUS
				dbo.IP_FLWSHT_REC
				dbo.IP_FLWSHT_MEAS
				dbo.IP_FLO_GP_DATA
				dbo.IP_FLT_DATA

  Temp tables :
                #ClinicPatient
				#ScheduledAppointmentReason
				#ScheduledAppointmentNote
                #ScheduledAppointment
                #ScheduledAppointmentPlus
                #ScheduledAppointmentLinked
                #ScheduledClinicAppointment
                #FLT
                #FLM
                #FLTPIVOT
                #FLMSummary
                #ScheduledClinicAppointmentDetail
                #RptgTemp

      OUTPUTS:
                CLARITY_App.Stage.iQueue_Clinics_Extract
----------------------------------------------------------------------------------------------------------------------------------------
MODS:     04/17/2019--TMB-- Create new stored procedure
          04/19/2019--WDR-- Changed APPT_NOTE to APPT_NOTES
          04/26/2019--WDR-- Convert commas in PROV_NAME and APPT_NOTES to ^ symbol.
		                    This is done so that the file can be sent as a comma-delimited file.
							Took out PAT_ENC_CSN_ID_unhashed for PHI reasons.
							Change future appts to look ahead 6 months. 
							Change EOD lag date to -3 days.
		  05/29/2019--TMB-- Add PROV_ID to extract.
		                    Add PROV_IDs to WHERE statement that defines pilot visit population
		  06/05/2019--WDR-- Output table renamed to Clarity_App.
		  08/06/2019--TMB-- Add UVPC DIGESTIVE HEALTH (10242051) to pilot department list.
		  01/28/2020--TMB-- Convert commas in ENC_REASON_NAME to ^ symbol.
*****************************************************************************************************************************************/

  SET NOCOUNT ON;

---------------------------------------------------
---Default date range is the prior day up to the following two months
  DECLARE @CurrDate SMALLDATETIME;

  SET @CurrDate = CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME);

  IF @StartDate IS NULL
      BEGIN
          -- EOD lag date
          SET @StartDate = CAST(CAST(DATEADD(DAY, -3, @CurrDate) AS DATE) AS SMALLDATETIME)
          + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME);
      END;
  IF @EndDate IS NULL
      BEGIN
          -- End of month, six months ahead from current date
          SET @EndDate = CAST(EOMONTH(@CurrDate, 6) AS SMALLDATETIME)
          + CAST(CAST('23:59:59' AS TIME) AS SMALLDATETIME);
      END;
----------------------------------------------------

IF OBJECT_ID('tempdb..#ClinicPatient ') IS NOT NULL
DROP TABLE #ClinicPatient

IF OBJECT_ID('tempdb..#ScheduledAppointmentReason ') IS NOT NULL
DROP TABLE #ScheduledAppointmentReason

IF OBJECT_ID('tempdb..#ScheduledAppointmentNote ') IS NOT NULL
DROP TABLE #ScheduledAppointmentNote

IF OBJECT_ID('tempdb..#ScheduledAppointment ') IS NOT NULL
DROP TABLE #ScheduledAppointment

IF OBJECT_ID('tempdb..#ScheduledAppointmentPlus ') IS NOT NULL
DROP TABLE #ScheduledAppointmentPlus

IF OBJECT_ID('tempdb..#ScheduledAppointmentLinked ') IS NOT NULL
DROP TABLE #ScheduledAppointmentLinked

IF OBJECT_ID('tempdb..#ScheduledClinicAppointment ') IS NOT NULL
DROP TABLE #ScheduledClinicAppointment

IF OBJECT_ID('tempdb..#FLT ') IS NOT NULL
DROP TABLE #FLT

IF OBJECT_ID('tempdb..#FLM ') IS NOT NULL
DROP TABLE #FLM

IF OBJECT_ID('tempdb..#FLTPIVOT ') IS NOT NULL
DROP TABLE #FLTPIVOT

IF OBJECT_ID('tempdb..#FLMSummary ') IS NOT NULL
DROP TABLE #FLMSummary

IF OBJECT_ID('tempdb..#ScheduledClinicAppointmentDetail ') IS NOT NULL
DROP TABLE #ScheduledClinicAppointmentDetail

IF OBJECT_ID('tempdb..#RptgTemp ') IS NOT NULL
DROP TABLE #RptgTemp

  -- Create temp table #ClinicPatient with PAT_ID and Appt date

  SELECT DISTINCT
     pa.PAT_ID
    ,CAST(pa.[APPT_DTTM] AS DATE) AS [Appt date]
	,pt.PAT_MRN_ID
  INTO #ClinicPatient
  FROM [CLARITY].[dbo].[V_SCHED_APPT] AS pa
  LEFT OUTER JOIN [CLARITY].[dbo].[PATIENT] AS pt
  ON pt.PAT_ID = pa.PAT_ID
  WHERE
  (pa.[APPT_DTTM] >= @StartDate AND pa.[APPT_DTTM] <= @EndDate)
   AND ((pa.DEPARTMENT_ID IN (10210002 -- ECCC HEM ONC WEST
                             ,10210030 -- ECCC NEURO WEST
							 ,10243003 -- UVHE DIGESTIVE HEALTH
							 ,10243087 -- UVHE SURG DIGESTIVE HL
							 ,10244023 -- UVWC MED GI CL
							 ,10242051 -- UVPC DIGESTIVE HEALTH
							 )
	    )
		OR
		(pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		               ,'28954' -- CROPLEY, THOMAS
					   ,'89921' -- ISHARWAL, SUMIT
					   ,'29044' -- KRUPSKI, TRACEY
					   ,'56655' -- MAITLAND, HILLARY S
					   ,'29690' -- SHAFFREY, MARK
					   )
		)
	   )


  -- Create index for temp table #ClinicPatient

  CREATE UNIQUE CLUSTERED INDEX IX_ClinicPaitent ON #ClinicPatient ([PAT_ID], [Appt date])

  -- Create temp table #ScheduledAppointmentReason

  SELECT PAT_ENC_RSN_VISIT.PAT_ENC_CSN_ID
       , CL_RSN_FOR_VISIT.REASON_VISIT_NAME AS ENC_REASON_NAME
	   , PAT_ENC_RSN_VISIT.LINE
  INTO #ScheduledAppointmentReason
  FROM (SELECT DISTINCT
			PAT_ID
        FROM #ClinicPatient) ClinicPatient
  INNER JOIN CLARITY.dbo.PAT_ENC_RSN_VISIT PAT_ENC_RSN_VISIT	ON ClinicPatient.PAT_ID = PAT_ENC_RSN_VISIT.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.CL_RSN_FOR_VISIT CL_RSN_FOR_VISIT ON CL_RSN_FOR_VISIT.REASON_VISIT_ID = PAT_ENC_RSN_VISIT.ENC_REASON_ID
  ORDER BY PAT_ENC_RSN_VISIT.PAT_ENC_CSN_ID
         , PAT_ENC_RSN_VISIT.LINE

  -- Create index for temp table #ScheduledAppointmentReason

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentReason ON #ScheduledAppointmentReason (PAT_ENC_CSN_ID, LINE)

  -- Create temp table #ScheduledAppointmentNote

  SELECT PAT_ENC_APPT_NOTES.PAT_ENC_CSN_ID
       , PAT_ENC_APPT_NOTES.APPT_NOTE
	   , PAT_ENC_APPT_NOTES.LINE
  INTO #ScheduledAppointmentNote
  FROM (SELECT DISTINCT
			PAT_ID
        FROM #ClinicPatient) ClinicPatient
  INNER JOIN CLARITY.dbo.PAT_ENC_APPT_NOTES	PAT_ENC_APPT_NOTES ON ClinicPatient.PAT_ID = PAT_ENC_APPT_NOTES.PAT_ID
  WHERE LEN(PAT_ENC_APPT_NOTES.APPT_NOTE) > 0
  ORDER BY PAT_ENC_APPT_NOTES.PAT_ENC_CSN_ID
         , PAT_ENC_APPT_NOTES.LINE

  -- Create index for temp table #ScheduledAppointmentNote

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentNote ON #ScheduledAppointmentNote (PAT_ENC_CSN_ID, LINE)

  -- Create temp table #ScheduledAppointment

  SELECT
     pa.PAT_ID
    ,CAST(pa.[APPT_DTTM]           AS DATE)          AS [Appointment Date]
    ,CAST(pa.[APPT_DTTM]           AS SMALLDATETIME) AS [APPT_DTTM]
    ,ROW_NUMBER() OVER (PARTITION BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE) ORDER BY pa.[APPT_DTTM]) AS SeqNbr
    ,ROW_NUMBER() OVER (PARTITION BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE) ORDER BY pa.[APPT_DTTM] DESC) AS SeqNbrDesc
    ,pa.PAT_ENC_CSN_ID                               AS [PAT_ENC_CSN_ID_unhashed]
	,HASHBYTES('SHA2_256',CAST(enc.PAT_ENC_CSN_ID AS VARCHAR(18))) AS [PAT_ENC_CSN_ID]
    ,pt.PAT_MRN_ID									 AS [PAT_MRN_ID_unhashed]
	,HASHBYTES('SHA2_256',CAST(pt.PAT_MRN_ID AS VARCHAR(10))) AS [PAT_MRN_ID]
	,pa.DEPARTMENT_ID
    ,LEAD(pa.[DEPARTMENT_ID]) OVER (PARTITION BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE) ORDER BY pa.[APPT_DTTM], pa.PAT_ENC_CSN_ID) AS [Next DEPARTMENT_ID]
    ,dep.DEPARTMENT_NAME                             AS [DEPARTMENT_NAME]
    ,pa.DEPT_SPECIALTY_NAME                          AS [DEPT_SPECIALTY_NAME]
    ,pa.PRC_NAME                                     AS [PRC_NAME]
	,encrsn.ENC_REASON_NAME
	,apptnote.APPT_NOTES
    ,pa.APPT_LENGTH                                  AS [APPT_LENGTH]
    ,pa.APPT_STATUS_C
    ,ZC_APPT_STATUS.NAME                             AS [APPT_STATUS_NAME]
    ,CAST(pa.CHECKIN_DTTM          AS SMALLDATETIME) AS [CHECKIN_DTTM]
    ,CAST(pa.ARVL_LIST_REMOVE_DTTM AS SMALLDATETIME) AS [ARVL_LIST_REMOVE_DTTM]
    ,CAST(pa.APPT_MADE_DATE        AS DATE)          AS [APPT_MADE_DATE]
    ,pa.APPT_MADE_DTTM                               AS [APPT_MADE_DTTM]
    ,CAST(pa.APPT_CANC_DATE        AS DATE)          AS [APPT_CANC_DATE]
    ,LEAD(pa.[APPT_STATUS_C]) OVER (PARTITION BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE) ORDER BY pa.[APPT_DTTM], pa.PAT_ENC_CSN_ID) AS [Next APPT_STATUS_C]
    ,enc.INPATIENT_DATA_ID
    ,ROW_NUMBER() OVER (ORDER BY pa.PAT_ID, CAST(pa.[APPT_DTTM] AS DATE), pa.[APPT_DTTM]) AS RecordId

  --
  -- Additional timestamps
  --
    ,pa.SIGNIN_DTTM
	,pa.PAGED_DTTM
	,pa.BEGIN_CHECKIN_DTTM
	,pa.ROOMED_DTTM
	,pa.FIRST_ROOM_ASSIGN_DTTM
	,pa.NURSE_LEAVE_DTTM
	,pa.PHYS_ENTER_DTTM
	,pa.VISIT_END_DTTM
	,pa.CHECKOUT_DTTM
	,pa.TIME_TO_ROOM_MINUTES --diff between check in time and roomed time (earliest of the ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, and FIRST_ROOM_ASSIGN_DTTM)
	,pa.TIME_IN_ROOM_MINUTES --diff between roomed time and appointment end time (earlier of the CHECKOUT_DTTM and VISIT_END_DTTM)
	,pa.CYCLE_TIME_MINUTES	 --diff between the check-in time and the appointment end time.
	,pa.APPT_CANC_DTTM
	,CASE
	  WHEN enc.ENC_TYPE_C IN (				-- FACE TO FACE UVA DEFINED ENCOUNTER TYPES
			                  '1001'			--Anti-coag visit
			                 ,'50'			--Appointment
			                 ,'213'			--Dentistry Visit
			                 ,'2103500001'	--Home Visit
			                 ,'3'			--Hospital Encounter
			                 ,'108'			--Immunization
			                 ,'1201'			--Initial Prenatal
			                 ,'101'			--Office Visit
			                 ,'2100700001'	--Office Visit / FC
			                 ,'1003'			--Procedure visit
			                 ,'1200'			--Routine Prenatal
			                 ) THEN 1
      ELSE 0
	 END AS F2F_Flag
	,REFERRAL.ENTRY_DATE
	,REFERRAL_HIST.CHANGE_DATE
	,pa.PROV_ID
	,CLARITY_SER.PROV_NAME
	,fpa.UPDATE_DATE

  INTO #ScheduledAppointment
  FROM [CLARITY].[dbo].[V_SCHED_APPT]     AS pa
  INNER JOIN #ClinicPatient               AS cp   ON pa.PAT_ID = cp.PAT_ID AND CAST(pa.[APPT_DTTM] AS DATE) = cp.[Appt Date]
  LEFT OUTER JOIN [CLARITY].[dbo].[F_SCHED_APPT]	AS fpa	ON fpa.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.PAT_ENC     AS enc   ON enc.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS dep   ON pa.DEPARTMENT_ID   = dep.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS CLARITY_SER   ON pa.PROV_ID = CLARITY_SER.PROV_ID
  LEFT OUTER JOIN (SELECT DISTINCT
                          rsn.PAT_ENC_CSN_ID
                        , (SELECT COALESCE(MAX(rsnt.ENC_REASON_NAME),'')  + '|' AS [text()]
		                   FROM #ScheduledAppointmentReason rsnt
						   WHERE rsnt.PAT_ENC_CSN_ID = rsn.PAT_ENC_CSN_ID
		                   GROUP BY rsnt.PAT_ENC_CSN_ID
				                  , rsnt.LINE
	                       FOR XML PATH ('')) AS ENC_REASON_NAME
                   FROM #ScheduledAppointmentReason AS rsn)	AS encrsn	ON encrsn.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN (SELECT DISTINCT
                          note.PAT_ENC_CSN_ID
                        , (SELECT COALESCE(MAX(notet.APPT_NOTE),'')  + '|' AS [text()]
		                   FROM #ScheduledAppointmentNote notet
						   WHERE notet.PAT_ENC_CSN_ID = note.PAT_ENC_CSN_ID
		                   GROUP BY notet.PAT_ENC_CSN_ID
				                  , notet.LINE
	                       FOR XML PATH ('')) AS APPT_NOTES
                   FROM #ScheduledAppointmentNote AS note)	AS apptnote	ON apptnote.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.REFERRAL    AS REFERRAL      ON REFERRAL.REFERRAL_ID = pa.REFERRAL_ID
  LEFT OUTER JOIN (SELECT REFERRAL_ID, CHANGE_DATE
				   FROM CLARITY.dbo.REFERRAL_HIST
				   WHERE CHANGE_TYPE_C = 1) AS REFERRAL_HIST    ON REFERRAL_HIST.REFERRAL_ID = REFERRAL.REFERRAL_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_STATUS	AS ZC_APPT_STATUS	ON ZC_APPT_STATUS.APPT_STATUS_C = enc.APPT_STATUS_C
  LEFT OUTER JOIN [CLARITY].[dbo].[PATIENT] AS pt	ON pt.PAT_ID = pa.PAT_ID
  WHERE ((pa.DEPARTMENT_ID IN (10210002 -- ECCC HEM ONC WEST
                             ,10210030 -- ECCC NEURO WEST
							 ,10243003 -- UVHE DIGESTIVE HEALTH
							 ,10243087 -- UVHE SURG DIGESTIVE HL
							 ,10244023 -- UVWC MED GI CL
							 ,10242051 -- UVPC DIGESTIVE HEALTH
							 )
	    )
		OR
		(pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		               ,'28954' -- CROPLEY, THOMAS
					   ,'89921' -- ISHARWAL, SUMIT
					   ,'29044' -- KRUPSKI, TRACEY
					   ,'56655' -- MAITLAND, HILLARY S
					   ,'29690' -- SHAFFREY, MARK
					   )
		)
	   )

  -- Create index for temp table #ScheduledAppointment

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointment ON #ScheduledAppointment ([PAT_ID], [Appointment Date], RecordId)

  -- Create temp table #ScheduledAppointmentPlus

  SELECT
     pa.PAT_ENC_CSN_ID
	,pa.PAT_ENC_CSN_ID_unhashed
    ,pa.PAT_MRN_ID
	,pa.PAT_MRN_ID_unhashed
    ,pa.DEPARTMENT_NAME
    ,pa.DEPT_SPECIALTY_NAME
    ,pa.PRC_NAME
	,pa.ENC_REASON_NAME
	,pa.APPT_NOTES
    ,pa.APPT_LENGTH
    ,pa.APPT_STATUS_C
    ,pa.APPT_STATUS_NAME
    ,pa.[Appointment Date]
    ,pa.[APPT_DTTM]
    ,pa.SeqNbr
	,pa.SeqNbrDesc
    ,pa.RecordId
    ,CASE WHEN ((pa.DEPARTMENT_ID IN (10210002 -- ECCC HEM ONC WEST
                                     ,10210030 -- ECCC NEURO WEST
							         ,10243003 -- UVHE DIGESTIVE HEALTH
							         ,10243087 -- UVHE SURG DIGESTIVE HL
							         ,10244023 -- UVWC MED GI CL
							         ,10242051 -- UVPC DIGESTIVE HEALTH
                                     )
				)
				OR
		        (pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		                       ,'28954' -- CROPLEY, THOMAS
					           ,'89921' -- ISHARWAL, SUMIT
					           ,'29044' -- KRUPSKI, TRACEY
					           ,'56655' -- MAITLAND, HILLARY S
					           ,'29690' -- SHAFFREY, MARK
					           )
		        )
			   )
			   AND pa.[Next DEPARTMENT_ID] = 10210004
			   AND pa.[Next APPT_STATUS_C] IN (1,2,6) THEN pa.RecordId + 1
          WHEN ((pa.DEPARTMENT_ID IN (10210002 -- ECCC HEM ONC WEST
                                     ,10210030 -- ECCC NEURO WEST
							         ,10243003 -- UVHE DIGESTIVE HEALTH
							         ,10243087 -- UVHE SURG DIGESTIVE HL
							         ,10244023 -- UVWC MED GI CL
							         ,10242051 -- UVPC DIGESTIVE HEALTH
                                     )
				)
				OR
		        (pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		                       ,'28954' -- CROPLEY, THOMAS
					           ,'89921' -- ISHARWAL, SUMIT
					           ,'29044' -- KRUPSKI, TRACEY
					           ,'56655' -- MAITLAND, HILLARY S
					           ,'29690' -- SHAFFREY, MARK
					           )
		        )
			   )
			   AND pa.[Next DEPARTMENT_ID] = 10210004
			   AND pa.[Next APPT_STATUS_C] IN (3,4,5) THEN
           (SELECT COALESCE(MIN(sa.RecordId),0) FROM #ScheduledAppointment AS sa
                WHERE sa.RecordId > pa.RecordId
                AND sa.PAT_ID = pa.PAT_ID
                AND sa.[Appointment Date] = pa.[Appointment Date]
			    AND sa.[DEPARTMENT_ID] = 10210004
                AND sa.[APPT_STATUS_C] IN (1,2,6)) -- (Scheduled,Completed,Arrived)
          ELSE 0
     END                               AS [Next RecordId]
    ,pa.CHECKIN_DTTM
    ,pa.ARVL_LIST_REMOVE_DTTM
    ,pa.APPT_MADE_DATE
	,pa.APPT_MADE_DTTM
    ,pa.APPT_CANC_DATE
    ,pa.PAT_ID
    ,pa.INPATIENT_DATA_ID
    ,pa.DEPARTMENT_ID
    ,pa.SIGNIN_DTTM
	,pa.PAGED_DTTM
	,pa.BEGIN_CHECKIN_DTTM
	,pa.ROOMED_DTTM
	,pa.FIRST_ROOM_ASSIGN_DTTM
	,pa.NURSE_LEAVE_DTTM
	,pa.PHYS_ENTER_DTTM
	,pa.VISIT_END_DTTM
	,pa.CHECKOUT_DTTM
	,pa.TIME_TO_ROOM_MINUTES
	,pa.TIME_IN_ROOM_MINUTES
	,pa.CYCLE_TIME_MINUTES
	,pa.APPT_CANC_DTTM
	,pa.F2F_Flag
	,pa.ENTRY_DATE
	,pa.CHANGE_DATE
	,pa.PROV_ID
	,pa.PROV_NAME
	,pa.UPDATE_DATE

  INTO #ScheduledAppointmentPlus
  FROM #ScheduledAppointment     AS pa

  -- Create index for temp table #ScheduledAppointmentPlus

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentPlus ON #ScheduledAppointmentPlus ([PAT_ID], [Appointment Date], [RecordId], [Next RecordId])

  -- Create temp table #ScheduledAppointmentLinked

  SELECT
     apptplus.PAT_ENC_CSN_ID
	,apptplus.PAT_ENC_CSN_ID_unhashed
    ,apptplus.PAT_MRN_ID
	,apptplus.PAT_MRN_ID_unhashed
    ,apptplus.DEPARTMENT_NAME
	,apptplus.DEPT_SPECIALTY_NAME
    ,apptplus.PRC_NAME
	,apptplus.ENC_REASON_NAME
	,apptplus.APPT_NOTES
    ,apptplus.APPT_LENGTH
    ,apptplus.APPT_STATUS_C
    ,apptplus.APPT_STATUS_NAME
    ,apptplus.[Appointment Date]
    ,apptplus.[APPT_DTTM]
    ,apptplus.CHECKIN_DTTM
    ,apptplus.ARVL_LIST_REMOVE_DTTM
    ,apptplus.APPT_MADE_DATE
	,apptplus.APPT_MADE_DTTM
    ,apptplus.APPT_CANC_DATE
    ,CASE WHEN ((apptplus.SeqNbrDesc = 1) OR ((apptplus.SeqNbrDesc > 1) AND (apptplus.[Next RecordId] = 0))) THEN NULL
          ELSE appt.PAT_ENC_CSN_ID
     END AS [Next PAT_ENC_CSN_ID]
    ,CASE WHEN ((apptplus.SeqNbrDesc = 1) OR ((apptplus.SeqNbrDesc > 1) AND (apptplus.[Next RecordId] = 0))) THEN NULL
          ELSE appt.PAT_ENC_CSN_ID_unhashed
     END AS [Next PAT_ENC_CSN_ID_unhashed]
    ,CASE WHEN ((apptplus.SeqNbrDesc = 1) OR ((apptplus.SeqNbrDesc > 1) AND (apptplus.[Next RecordId] = 0))) THEN NULL
          ELSE appt.[APPT_DTTM]
     END AS [Next Appointment Time]
    ,CASE WHEN ((apptplus.SeqNbrDesc = 1) OR ((apptplus.SeqNbrDesc > 1) AND (apptplus.[Next RecordId] = 0))) THEN NULL
          ELSE appt.APPT_LENGTH
     END AS [Next APPT_LENGTH]
    ,CASE WHEN ((apptplus.SeqNbrDesc = 1) OR ((apptplus.SeqNbrDesc > 1) AND (apptplus.[Next RecordId] = 0))) THEN NULL
          ELSE appt.DEPARTMENT_ID
     END AS [Next DEPARTMENT_ID]
    ,apptplus.PAT_ID
    ,apptplus.INPATIENT_DATA_ID
    ,apptplus.DEPARTMENT_ID
    ,apptplus.SIGNIN_DTTM
	,apptplus.PAGED_DTTM
	,apptplus.BEGIN_CHECKIN_DTTM
	,apptplus.ROOMED_DTTM
	,apptplus.FIRST_ROOM_ASSIGN_DTTM
	,apptplus.NURSE_LEAVE_DTTM
	,apptplus.PHYS_ENTER_DTTM
	,apptplus.VISIT_END_DTTM
	,apptplus.CHECKOUT_DTTM
	,apptplus.TIME_TO_ROOM_MINUTES
	,apptplus.TIME_IN_ROOM_MINUTES
	,apptplus.CYCLE_TIME_MINUTES
	,apptplus.APPT_CANC_DTTM
	,apptplus.F2F_Flag
	,apptplus.ENTRY_DATE
	,apptplus.CHANGE_DATE
	,apptplus.PROV_ID
	,apptplus.PROV_NAME
	,apptplus.UPDATE_DATE

  INTO #ScheduledAppointmentLinked
  FROM #ScheduledAppointmentPlus        AS apptplus
  LEFT OUTER JOIN #ScheduledAppointment AS appt ON appt.RecordId = apptplus.[Next RecordId]

  -- Create index for temp table #ScheduledAppointmentLinked

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentLinked ON #ScheduledAppointmentLinked ([PAT_ID], [PAT_ENC_CSN_ID_unhashed], [APPT_DTTM])

  -- Create temp table #ScheduledClinicAppointment

  SELECT
     pa.PAT_ENC_CSN_ID
	,pa.PAT_ENC_CSN_ID_unhashed
    ,pa.PAT_MRN_ID
	,pa.PAT_MRN_ID_unhashed
    ,pa.DEPARTMENT_NAME
    ,pa.DEPT_SPECIALTY_NAME
    ,pa.PRC_NAME
	,pa.ENC_REASON_NAME
	,pa.APPT_NOTES
    ,pa.APPT_LENGTH
    ,pa.APPT_STATUS_C
    ,pa.APPT_STATUS_NAME
    ,pa.[Appointment Date]
    ,pa.[APPT_DTTM]
    ,pa.CHECKIN_DTTM
    ,pa.ARVL_LIST_REMOVE_DTTM
    ,pa.APPT_MADE_DATE
	,pa.APPT_MADE_DTTM
    ,pa.APPT_CANC_DATE
    ,pa.[Next PAT_ENC_CSN_ID]
	,pa.[Next PAT_ENC_CSN_ID_unhashed]
    ,pa.[Next Appointment Time]
    ,pa.[Next APPT_LENGTH]
	,pa.[Next DEPARTMENT_ID]
    ,pa.PAT_ID
    ,pa.INPATIENT_DATA_ID
    ,pa.DEPARTMENT_ID
    ,pa.SIGNIN_DTTM
	,pa.PAGED_DTTM
	,pa.BEGIN_CHECKIN_DTTM
	,pa.ROOMED_DTTM
	,pa.FIRST_ROOM_ASSIGN_DTTM
	,pa.NURSE_LEAVE_DTTM
	,pa.PHYS_ENTER_DTTM
	,pa.VISIT_END_DTTM
	,pa.CHECKOUT_DTTM
	,pa.TIME_TO_ROOM_MINUTES
	,pa.TIME_IN_ROOM_MINUTES
	,pa.CYCLE_TIME_MINUTES
	,pa.APPT_CANC_DTTM
	,pa.F2F_Flag
	,pa.ENTRY_DATE
	,pa.CHANGE_DATE
	,pa.PROV_ID
	,pa.PROV_NAME
	,pa.UPDATE_DATE

  INTO #ScheduledClinicAppointment
  FROM #ScheduledAppointmentLinked AS pa
  WHERE
  ((pa.DEPARTMENT_ID IN (10210002 -- ECCC HEM ONC WEST
                        ,10210030 -- ECCC NEURO WEST
					    ,10243003 -- UVHE DIGESTIVE HEALTH
						,10243087 -- UVHE SURG DIGESTIVE HL
						,10244023 -- UVWC MED GI CL
					    ,10242051 -- UVPC DIGESTIVE HEALTH
						)
   )
   OR
   (pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		          ,'28954' -- CROPLEY, THOMAS
				  ,'89921' -- ISHARWAL, SUMIT
				  ,'29044' -- KRUPSKI, TRACEY
				  ,'56655' -- MAITLAND, HILLARY S
				  ,'29690' -- SHAFFREY, MARK
				  )
   )
  )

  -- Create index for temp table #ScheduledClinicAppointment

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledClinicAppointment ON #ScheduledClinicAppointment ([PAT_ID], [PAT_ENC_CSN_ID_unhashed], [APPT_DTTM])

  -- Create temp table #FLT

  SELECT DISTINCT
     appt.PAT_ENC_CSN_ID_unhashed
    ,appt.PAT_ID
    ,appt.INPATIENT_DATA_ID
    ,flt.RECORDED_TIME
	,flt.FLT_ID
    ,flt.FLO_MEAS_ID
	,flt.TEMPLATE_NAME
    ,flt.MEAS_VALUE
    ,ROW_NUMBER() OVER (PARTITION BY appt.PAT_ID, appt.[PAT_ENC_CSN_ID_unhashed], flt.FLT_ID ORDER BY flt.RECORDED_TIME) AS SeqNbr
  INTO #FLT
  FROM (SELECT DISTINCT
               [PAT_ENC_CSN_ID_unhashed]
             , PAT_ID
             , INPATIENT_DATA_ID
        FROM #ScheduledClinicAppointment) appt
  INNER JOIN (SELECT
                 fr.PAT_ID
                ,fm.RECORDED_TIME
                ,fm.MEAS_VALUE
                ,CASE WHEN fm.FLT_ID IN ('2100300004','31010','31000') THEN '2100300004' ELSE fm.FLT_ID END AS FLT_ID
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
				  OR
			      (fm.FLO_MEAS_ID IN ('3506','3507') -- AMB PATIENT VERIFIED
                   AND fm.MEAS_VALUE = 'Yes')
				  OR
			      (fm.FLT_ID IN ('2100300004','31010','31000')) -- UVA AMB VITALS SIMPLE, UVA IP VITALS ICU, UVA IP VITALS SIMPLE
             ) flt
  ON appt.INPATIENT_DATA_ID = flt.INPATIENT_DATA_ID

  -- Create index for temp table #FLT

  CREATE UNIQUE CLUSTERED INDEX IX_FLT ON #FLT (PAT_ENC_CSN_ID_unhashed, FLT_ID, SeqNbr)

  -- Create temp table #FLM

  SELECT DISTINCT
     appt.[PAT_ENC_CSN_ID_unhashed]
    ,appt.PAT_ID
    ,appt.INPATIENT_DATA_ID
    ,flm.RECORDED_TIME
    ,flm.FLO_MEAS_ID
    ,COALESCE(flm.MEAS_VALUE,'NA') AS MEAS_VALUE
    ,ROW_NUMBER() OVER (PARTITION BY appt.PAT_ID, appt.[PAT_ENC_CSN_ID_unhashed], flm.FLO_MEAS_ID ORDER BY flm.RECORDED_TIME) AS SeqNbrAsc
    ,ROW_NUMBER() OVER (PARTITION BY appt.PAT_ID, appt.[PAT_ENC_CSN_ID_unhashed], flm.FLO_MEAS_ID ORDER BY flm.RECORDED_TIME DESC) AS SeqNbrDesc
	,COUNT(flm.MEAS_VALUE) over(partition by appt.PAT_ID, appt.[PAT_ENC_CSN_ID_unhashed], flm.FLO_MEAS_ID) as Meas_Count
	,COUNT(*) over(partition by appt.PAT_ID, appt.[PAT_ENC_CSN_ID_unhashed], COALESCE(flm.MEAS_VALUE,'NA'), flm.MEAS_VALUE) as Meas_Value_Count
  INTO #FLM
  FROM (SELECT DISTINCT
               [PAT_ENC_CSN_ID_unhashed]
             , PAT_ID
             , INPATIENT_DATA_ID
        FROM #ScheduledClinicAppointment) appt
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
			      (fm.FLO_MEAS_ID ='4143') -- T UVA PATIENT TRACKING - R UVA AMB PATIENT ROOM
				  OR
			      (fm.FLO_MEAS_ID = '4147') -- T UVA PATIENT TRACKING - R UVA AMB PATIENT TRACK VERSION 2
             ) flm
  ON appt.INPATIENT_DATA_ID = flm.INPATIENT_DATA_ID

  -- Create index for temp table #FLM

  --CREATE UNIQUE CLUSTERED INDEX IX_FLM ON #FLM ([Appointment ID], FLO_MEAS_ID, MEAS_VALUE, RECORDED_TIME)
  --CREATE NONCLUSTERED INDEX IX_FLM ON #FLM ([Appointment ID], FLO_MEAS_ID, MEAS_VALUE, RECORDED_TIME)

  -- Create temp table #FLTPIVOT

  SELECT
     PAT_ID
   , [PAT_ENC_CSN_ID_unhashed]
   , [2100000001] AS [AMB PATIENT VERIFIED]
   , [2103800001] AS [T UVA AMB PATIENT UNDERSTANDING AVS]
   , [2100300004] AS [UVA AMB VITALS SIMPLE]
  INTO #FLTPIVOT
  FROM
  (SELECT PAT_ID
        , [PAT_ENC_CSN_ID_unhashed]
        , FLT_ID
		, RECORDED_TIME
   FROM #FLT
   WHERE SeqNbr = 1) FlwSht
  PIVOT
  (
  MAX(RECORDED_TIME)
  FOR FLT_ID IN ([2100000001] -- AMB PATIENT VERIFIED
			   , [2103800001] -- T UVA AMB PATIENT UNDERSTANDING AVS
			   , [2100300004] -- UVA AMB VITALS SIMPLE
			    )
  ) AS PivotTable

  -- Create index for temp table #FLTPIVOT

  CREATE UNIQUE CLUSTERED INDEX IX_FLTPIVOT ON #FLTPIVOT (PAT_ENC_CSN_ID_unhashed)

  -- Create temp table #FLMSummary

  SELECT PAT_ID
       , PAT_ENC_CSN_ID_unhashed
	   , (SELECT MAX(flmt.MEAS_VALUE) + '|' AS [text()]
		  FROM #FLM flmt
		  WHERE flmt.PAT_ID = flm.PAT_ID
		  AND flmt.PAT_ENC_CSN_ID_unhashed = flm.PAT_ENC_CSN_ID_unhashed
		  AND flmt.FLO_MEAS_ID = '4143'
		  GROUP BY flmt.RECORDED_TIME
	      FOR XML PATH ('')
	     ) AS Patient_Room
	   , (SELECT LEFT(CONVERT(VARCHAR(19),MAX(flmt.RECORDED_TIME),120),16)  + '|' AS [text()]
		  FROM #FLM flmt
		  WHERE flmt.PAT_ID = flm.PAT_ID
		  AND flmt.PAT_ENC_CSN_ID_unhashed = flm.PAT_ENC_CSN_ID_unhashed
		  AND flmt.FLO_MEAS_ID = '4143'
		  GROUP BY flmt.FLO_MEAS_ID
		         , flmt.RECORDED_TIME
	      FOR XML PATH ('')
	     ) AS Patient_Room_Recorded_DtTm
	   , (SELECT MAX(flmt.MEAS_VALUE) + '|' AS [text()]
		  FROM #FLM flmt
		  WHERE flmt.PAT_ID = flm.PAT_ID
		  AND flmt.PAT_ENC_CSN_ID_unhashed = flm.PAT_ENC_CSN_ID_unhashed
		  AND flmt.FLO_MEAS_ID = '4147'
		  GROUP BY flmt.RECORDED_TIME
	      FOR XML PATH ('')
	     ) AS Patient_Track
	   , (SELECT LEFT(CONVERT(VARCHAR(19),MAX(flmt.RECORDED_TIME),120),16)  + '|' AS [text()]
		  FROM #FLM flmt
		  WHERE flmt.PAT_ID = flm.PAT_ID
		  AND flmt.PAT_ENC_CSN_ID_unhashed = flm.PAT_ENC_CSN_ID_unhashed
		  AND flmt.FLO_MEAS_ID = '4147'
		  GROUP BY flmt.FLO_MEAS_ID
		         , flmt.RECORDED_TIME
	      FOR XML PATH ('')
	     ) AS Patient_Track_Recorded_DtTm
  INTO #FLMSummary
  FROM #FLM flm
  GROUP BY PAT_ID
         , PAT_ENC_CSN_ID_unhashed

  -- Create index for temp table #FLMSummary

  CREATE UNIQUE CLUSTERED INDEX IX_FLMSummary ON #FLMSummary (PAT_ID, PAT_ENC_CSN_ID_unhashed)

  -- Create temp table #ScheduledClinicAppointmentDetail

  SELECT
     pa.PAT_ID
    ,pa.PAT_ENC_CSN_ID
	,pa.PAT_ENC_CSN_ID_unhashed
    ,pa.PAT_MRN_ID
	,pa.PAT_MRN_ID_unhashed
    ,pa.DEPARTMENT_NAME
    ,pa.DEPT_SPECIALTY_NAME
    ,pa.PRC_NAME
	,pa.ENC_REASON_NAME
	,pa.APPT_NOTES
    ,pa.APPT_LENGTH
    ,pa.APPT_STATUS_C
    ,pa.APPT_STATUS_NAME
    ,pa.[Appointment Date]
    ,pa.[APPT_DTTM]
    ,pa.CHECKIN_DTTM
	,flm.Patient_Room
	,flm.Patient_Room_Recorded_DtTm
	,flm.Patient_Track
	,flm.Patient_Track_Recorded_DtTm
    ,pa.ARVL_LIST_REMOVE_DTTM
    ,CAST(flt.[AMB PATIENT VERIFIED] AS SMALLDATETIME) AS [AMB PATIENT VERIFIED]
    ,CAST(flt.[T UVA AMB PATIENT UNDERSTANDING AVS] AS SMALLDATETIME) AS [T UVA AMB PATIENT UNDERSTANDING AVS]
    ,CAST(flt.[UVA AMB VITALS SIMPLE] AS SMALLDATETIME) AS [UVA AMB VITALS SIMPLE]
    ,pa.APPT_MADE_DATE
	,pa.APPT_MADE_DTTM
    ,pa.APPT_CANC_DATE
    ,pa.[Next PAT_ENC_CSN_ID]
	,pa.[Next PAT_ENC_CSN_ID_unhashed]
    ,pa.[Next Appointment Time]
    ,pa.[Next APPT_LENGTH]
	,pa.[Next DEPARTMENT_ID]
    ,pa.INPATIENT_DATA_ID
    ,pa.DEPARTMENT_ID
    ,pa.SIGNIN_DTTM
	,pa.PAGED_DTTM
	,pa.BEGIN_CHECKIN_DTTM
	,pa.ROOMED_DTTM
	,pa.FIRST_ROOM_ASSIGN_DTTM
	,pa.NURSE_LEAVE_DTTM
	,pa.PHYS_ENTER_DTTM
	,pa.VISIT_END_DTTM
	,pa.CHECKOUT_DTTM
	,pa.TIME_TO_ROOM_MINUTES
	,pa.TIME_IN_ROOM_MINUTES
	,pa.CYCLE_TIME_MINUTES
	,pa.APPT_CANC_DTTM
	,pa.F2F_Flag
	,pa.ENTRY_DATE
	,pa.CHANGE_DATE
	,pa.PROV_ID
	,pa.PROV_NAME
	,pa.UPDATE_DATE
  INTO #ScheduledClinicAppointmentDetail
  FROM #ScheduledClinicAppointment AS pa
  LEFT OUTER JOIN (SELECT PAT_ID
                        , PAT_ENC_CSN_ID_unhashed
						, Patient_Room
						, Patient_Room_Recorded_DtTm
						, Patient_Track
						, Patient_Track_Recorded_DtTm
                   FROM #FLMSummary) flm
  ON ((flm.PAT_ID = pa.PAT_ID) AND (flm.PAT_ENC_CSN_ID_unhashed = pa.PAT_ENC_CSN_ID_unhashed))
  LEFT OUTER JOIN (SELECT [PAT_ENC_CSN_ID_unhashed]
                        , [AMB PATIENT VERIFIED]
                        , [T UVA AMB PATIENT UNDERSTANDING AVS]
						, [UVA AMB VITALS SIMPLE]
                   FROM #FLTPIVOT) flt
  ON flt.[PAT_ENC_CSN_ID_unhashed] = pa.[PAT_ENC_CSN_ID_unhashed]

  -- Create temp table #RptgTemp

  SELECT
     [PAT_ENC_CSN_ID_unhashed]
    ,[PAT_ENC_CSN_ID]
	,[PAT_MRN_ID]
    ,[DEPARTMENT_NAME]
    ,[DEPT_SPECIALTY_NAME]
	,[PROV_ID]
	,[PROV_NAME]
    ,[APPT_DTTM]
	,[ENC_REASON_NAME]
	,[APPT_NOTES]
	,[PRC_NAME]
	,[APPT_LENGTH]
	,[APPT_STATUS_NAME]
	,[APPT_MADE_DTTM]
	,[APPT_CANC_DTTM]
	,[UPDATE_DATE]
	,[SIGNIN_DTTM]
	,[BEGIN_CHECKIN_DTTM]
	,[CHECKIN_DTTM]
	,[Patient_Room]
	,[Patient_Room_Recorded_DtTm]
	,[Patient_Track]
	,[Patient_Track_Recorded_DtTm]
	,[ARVL_LIST_REMOVE_DTTM]
    ,[AMB PATIENT VERIFIED]
	,[UVA AMB VITALS SIMPLE]
	,[ROOMED_DTTM]
	,[NURSE_LEAVE_DTTM]
	,[PHYS_ENTER_DTTM]
    ,[T UVA AMB PATIENT UNDERSTANDING AVS]
	,[VISIT_END_DTTM]
	,[CHECKOUT_DTTM]
	,[TIME_TO_ROOM_MINUTES]
	,[TIME_IN_ROOM_MINUTES]
	,[CYCLE_TIME_MINUTES]
    ,CASE WHEN [Next PAT_ENC_CSN_ID] IS NULL THEN '0' ELSE '1' END AS [Linked Appointment Flag]
    ,'Rptg.uspSrc_iQueue_Outpatient_Daily' AS [ETL_guid]
    ,GETDATE()                             AS Load_Dte
  INTO #RptgTemp FROM
   (
    SELECT
	   [PAT_ENC_CSN_ID_unhashed]
      ,[PAT_ENC_CSN_ID]
	  ,[PAT_MRN_ID]
      ,[DEPARTMENT_NAME]
      ,[DEPT_SPECIALTY_NAME]
	  ,[PROV_ID]
	  ,[PROV_NAME]
      ,[APPT_DTTM]
	  ,[ENC_REASON_NAME]
	  ,[APPT_NOTES]
	  ,[PRC_NAME]
	  ,[APPT_LENGTH]
	  ,[APPT_STATUS_NAME]
	  ,[APPT_MADE_DTTM]
	  ,[APPT_CANC_DTTM]
	  ,[UPDATE_DATE]
	  ,[SIGNIN_DTTM]
	  ,[BEGIN_CHECKIN_DTTM]
	  ,[CHECKIN_DTTM]
	  ,[Patient_Room]
	  ,[Patient_Room_Recorded_DtTm]
	  ,[Patient_Track]
	  ,[Patient_Track_Recorded_DtTm]
	  ,[ARVL_LIST_REMOVE_DTTM]
      ,[AMB PATIENT VERIFIED]
	  ,[UVA AMB VITALS SIMPLE]
	  ,[ROOMED_DTTM]
	  ,[NURSE_LEAVE_DTTM]
	  ,[PHYS_ENTER_DTTM]
      ,[T UVA AMB PATIENT UNDERSTANDING AVS]
	  ,[VISIT_END_DTTM]
	  ,[CHECKOUT_DTTM]
	  ,[TIME_TO_ROOM_MINUTES]
	  ,[TIME_IN_ROOM_MINUTES]
	  ,[CYCLE_TIME_MINUTES]
	  ,[Next PAT_ENC_CSN_ID]
    FROM #ScheduledClinicAppointmentDetail AS pa
   ) A

  -- Put contents of temp table #RptgTemp into db table

  INSERT INTO CLARITY_App_Dev.Stage.iQueue_Clinics_Extract
  (
      PAT_ENC_CSN_ID_unhashed,PAT_ENC_CSN_ID,PAT_MRN_ID,DEPARTMENT_NAME,DEPT_SPECIALTY_NAME,
      PROV_ID,PROV_NAME,APPT_DTTM,ENC_REASON_NAME,APPT_NOTES,PRC_NAME,APPT_LENGTH,APPT_STATUS_NAME,
      APPT_MADE_DTTM,APPT_CANC_DTTM,UPDATE_DATE,SIGNIN_DTTM,BEGIN_CHECKIN_DTTM,CHECKIN_DTTM,
      Patient_Room,Patient_Room_Recorded_DtTm,Patient_Track,Patient_Track_Recorded_DtTm,
      ARVL_LIST_REMOVE_DTTM,[AMB PATIENT VERIFIED],[UVA AMB VITALS SIMPLE],ROOMED_DTTM,
      NURSE_LEAVE_DTTM,PHYS_ENTER_DTTM,[T UVA AMB PATIENT UNDERSTANDING AVS],VISIT_END_DTTM,
      CHECKOUT_DTTM,TIME_TO_ROOM_MINUTES,TIME_IN_ROOM_MINUTES,CYCLE_TIME_MINUTES,ETL_guid,
      Load_Dte
  )

  SELECT
    [PAT_ENC_CSN_ID_unhashed]                          -- this will be in the table, but not in the flat file
   ,ISNULL(CONVERT(VARCHAR(256),[PAT_ENC_CSN_ID],2),'')					AS [PAT_ENC_CSN_ID]
   ,ISNULL(CONVERT(VARCHAR(256),[PAT_MRN_ID],2),'')						AS [PAT_MRN_ID]
   ,ISNULL(CONVERT(VARCHAR(254),[DEPARTMENT_NAME]),'')					AS [DEPARTMENT_NAME]
   ,ISNULL(CONVERT(VARCHAR(254),[DEPT_SPECIALTY_NAME]),'')				AS [DEPT_SPECIALTY_NAME]
   ,ISNULL(CONVERT(VARCHAR(18),[PROV_ID]),'')							AS [PROV_ID]
    ,CASE
       WHEN [PROV_NAME] IS NULL  THEN CAST('' AS VARCHAR(200))
       ELSE CAST(REPLACE([PROV_NAME],',','^') AS VARCHAR(200))
     END                                                                AS [PROV_NAME]
   ,[APPT_DTTM]
   ,CASE
      WHEN CONVERT(VARCHAR(1200),LEFT([ENC_REASON_NAME],LEN([ENC_REASON_NAME])-1)) IS NULL THEN CAST(''  AS VARCHAR(1200))
      ELSE CAST(REPLACE(CONVERT(VARCHAR(1200),LEFT([ENC_REASON_NAME],LEN([ENC_REASON_NAME])-1)),',','^') AS VARCHAR(1200))
    END                                                                 AS [ENC_REASON_NAME]
    ,CASE
       WHEN [APPT_NOTES] IS NULL THEN CAST(''  AS VARCHAR(1200))
       ELSE CAST(REPLACE([APPT_NOTES],',','^') AS VARCHAR(1200))
     END                                                                AS [APPT_NOTES]
   ,ISNULL(CONVERT(VARCHAR(254),[PRC_NAME]),'')							AS [PRC_NAME]
   ,[APPT_LENGTH]
   ,ISNULL(CONVERT(VARCHAR(254),[APPT_STATUS_NAME]),'')					AS [APPT_STATUS_NAME]
   ,[APPT_MADE_DTTM]
   ,[APPT_CANC_DTTM]
   ,[UPDATE_DATE]
   ,[SIGNIN_DTTM]
   ,[BEGIN_CHECKIN_DTTM]
   ,[CHECKIN_DTTM]
   ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Room],LEN([Patient_Room])-1)),'')				AS [Patient_Room]
   ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Room_Recorded_DtTm],LEN([Patient_Room_Recorded_DtTm])-1)),'')				AS [Patient_Room_Recorded_DtTm]
   ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Track],LEN([Patient_Track])-1)),'')				AS [Patient_Track]
   ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Track_Recorded_DtTm],LEN([Patient_Track_Recorded_DtTm])-1)),'')				AS [Patient_Track_Recorded_DtTm]
   ,[ARVL_LIST_REMOVE_DTTM]
   ,[AMB PATIENT VERIFIED]
   ,[UVA AMB VITALS SIMPLE]
   ,[ROOMED_DTTM]
   ,[NURSE_LEAVE_DTTM]
   ,[PHYS_ENTER_DTTM]
   ,[T UVA AMB PATIENT UNDERSTANDING AVS]
   ,[VISIT_END_DTTM]
   ,[CHECKOUT_DTTM]
   ,[TIME_TO_ROOM_MINUTES]
   ,[TIME_IN_ROOM_MINUTES]
   ,[CYCLE_TIME_MINUTES]
   ,[ETL_guid]
   ,[Load_Dte]
  FROM #RptgTemp
  ORDER BY [APPT_DTTM]

  --SELECT
  --  --ISNULL(CONVERT(VARCHAR(254),[PAT_ENC_CSN_ID]),'')					AS [PAT_ENC_CSN_ID]
  --  ISNULL(CONVERT(VARCHAR(256),[PAT_ENC_CSN_ID],2),'')					AS [PAT_ENC_CSN_ID]
  -- --,PAT_ENC_CSN_ID_unhashed
  -- ,ISNULL(CONVERT(VARCHAR(256),[PAT_MRN_ID],2),'')						AS [PAT_MRN_ID]
  -- ,ISNULL(CONVERT(VARCHAR(254),[DEPARTMENT_NAME]),'')					AS [DEPARTMENT_NAME]
  -- ,ISNULL(CONVERT(VARCHAR(254),[DEPT_SPECIALTY_NAME]),'')				AS [DEPT_SPECIALTY_NAME]
  -- ,ISNULL(CONVERT(VARCHAR(200),[PROV_ID]),'')							AS [PROV_ID]
  -- ,ISNULL(CONVERT(VARCHAR(200),[PROV_NAME]),'')						AS [PROV_NAME]
  -- ,ISNULL(CONVERT(VARCHAR(19),[APPT_DTTM],121),'')						AS [APPT_DTTM]
  -- --,ISNULL(CONVERT(VARCHAR(254),LEFT([ENC_REASON_NAME],LEN([ENC_REASON_NAME])-1)),'')			AS [ENC_REASON_NAME]
  -- ,CASE
  --    WHEN CONVERT(VARCHAR(1200),LEFT([ENC_REASON_NAME],LEN([ENC_REASON_NAME])-1)) IS NULL THEN CAST(''  AS VARCHAR(1200))
  --    ELSE CAST(REPLACE(CONVERT(VARCHAR(1200),LEFT([ENC_REASON_NAME],LEN([ENC_REASON_NAME])-1)),',','^') AS VARCHAR(1200))
  --  END                                                                 AS [ENC_REASON_NAME]
  -- ,ISNULL(CONVERT(VARCHAR(254),LEFT([APPT_NOTES],LEN([APPT_NOTES])-1)),'')						AS [APPT_NOTES]
  -- ,ISNULL(CONVERT(VARCHAR(254),[PRC_NAME]),'')							AS [PRC_NAME]
  -- ,ISNULL(CONVERT(VARCHAR(18),[APPT_LENGTH]),'')						AS [APPT_LENGTH]
  -- ,ISNULL(CONVERT(VARCHAR(254),[APPT_STATUS_NAME]),'')					AS [APPT_STATUS_NAME]
  -- ,ISNULL(CONVERT(VARCHAR(19),[APPT_MADE_DTTM],121),'')				AS [APPT_MADE_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[APPT_CANC_DTTM],121),'')				AS [APPT_CANC_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[UPDATE_DATE],121),'')					AS [UPDATE_DATE]
  -- ,ISNULL(CONVERT(VARCHAR(19),[SIGNIN_DTTM],121),'')					AS [SIGNIN_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[BEGIN_CHECKIN_DTTM],121),'')			AS [BEGIN_CHECKIN_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[CHECKIN_DTTM],121),'')					AS [CHECKIN_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Room],LEN([Patient_Room])-1)),'')					AS [Patient_Room]
  -- ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Room_Recorded_DtTm],LEN([Patient_Room_Recorded_DtTm])-1)),'')					AS [Patient_Room_Recorded_DtTm]
  -- ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Track],LEN([Patient_Track])-1)),'')				AS [Patient_Track]
  -- ,ISNULL(CONVERT(VARCHAR(1200),LEFT([Patient_Track_Recorded_DtTm],LEN([Patient_Track_Recorded_DtTm])-1)),'')				AS [Patient_Track_Recorded_DtTm]
  -- ,ISNULL(CONVERT(VARCHAR(19),[ARVL_LIST_REMOVE_DTTM],121),'')			AS [ARVL_LIST_REMOVE_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[AMB PATIENT VERIFIED],121),'')			AS [AMB PATIENT VERIFIED]
  -- ,ISNULL(CONVERT(VARCHAR(19),[UVA AMB VITALS SIMPLE],121),'')			AS [UVA AMB VITALS SIMPLE]
  -- ,ISNULL(CONVERT(VARCHAR(19),[ROOMED_DTTM],121),'')					AS [ROOMED_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[NURSE_LEAVE_DTTM],121),'')				AS [NURSE_LEAVE_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[PHYS_ENTER_DTTM],121),'')				AS [PHYS_ENTER_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[T UVA AMB PATIENT UNDERSTANDING AVS],121),'')					AS [T UVA AMB PATIENT UNDERSTANDING AVS]
  -- ,ISNULL(CONVERT(VARCHAR(19),[VISIT_END_DTTM],121),'')				AS [VISIT_END_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(19),[CHECKOUT_DTTM],121),'')					AS [CHECKOUT_DTTM]
  -- ,ISNULL(CONVERT(VARCHAR(18),[TIME_TO_ROOM_MINUTES]),'')				AS [TIME_TO_ROOM_MINUTES]
  -- ,ISNULL(CONVERT(VARCHAR(18),[TIME_IN_ROOM_MINUTES]),'')				AS [TIME_IN_ROOM_MINUTES]
  -- ,ISNULL(CONVERT(VARCHAR(18),[CYCLE_TIME_MINUTES]),'')				AS [CYCLE_TIME_MINUTES]
  -- --,ISNULL(CONVERT(CHAR(1),[Linked Appointment Flag]),'')			    AS [Linked Appointment Flag]
  -- ,[ETL_guid]
  -- ,CONVERT(VARCHAR(19),[Load_Dte],121) AS [Load_Dte]
  --FROM #RptgTemp
  ----WHERE [APPT_STATUS_NAME] IN ('Arrived','Completed')
  ----ORDER BY [APPT_DTTM]
  ----ORDER BY CAST([APPT_DTTM] AS DATE)
  ----       , APPT_STATUS_NAME
  --ORDER BY DEPARTMENT_NAME
  --       , APPT_DTTM
  ----ORDER BY [T UVA AMB PATIENT UNDERSTANDING AVS]
  ----ORDER BY PAT_ENC_CSN_ID_unhashed
  ----ORDER BY PROV_NAME
  ----       , DEPARTMENT_NAME
  ----       , APPT_DTTM
  ----ORDER BY PROV_ID
  ----       , DEPARTMENT_NAME
  ----       , APPT_DTTM
  ----ORDER BY APPT_DTTM
  ----       , PAT_ENC_CSN_ID_unhashed

GO



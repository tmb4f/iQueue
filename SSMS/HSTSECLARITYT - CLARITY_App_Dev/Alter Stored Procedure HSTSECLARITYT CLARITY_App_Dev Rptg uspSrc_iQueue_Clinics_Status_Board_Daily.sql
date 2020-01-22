USE [CLARITY_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [Rptg].[uspSrc_iQueue_Clinics_Status_Board_Daily]
       (
        @StartDate SMALLDATETIME = NULL
       ,@EndDate SMALLDATETIME = NULL)
AS
/****************************************************************************************************************************************
WHAT: Create procedure Rptg.uspSrc_iQueue_Clinics_Status_Board_Daily
WHO : Tom Burgan
WHEN: 11/05/2019
WHY : Daily feed of patient flow data for Ambulatory Optimization iQueue project
----------------------------------------------------------------------------------------------------------------------------------------
INFO:
      INPUTS:   dbo.V_SCHED_APPT
	            dbo.PATIENT
				dbo.PAT_ENC_APPT_NOTES
				dbo.SMRTDTA_ELEM_DATA
				dbo.SMRTDTA_ELEM_VALUE
				dbo.HSP_INFECTION_PL
				dbo.ZC_INFECTION
				dbo.F_SCHED_APPT
				dbo.PAT_ENC
				dbo.CLARITY_DEP
				dbo.CLARITY_SER
				dbo.ZC_APPT_STATUS
				dbo.ZC_DISP_ENC_TYPE
				dbo.ZC_LANGUAGE
				dbo.ZC_INTRP_ASSIGNMEN
				dbo.ZC_ASGND_INTERP_TY
				dbo.ZC_INTERPRETER_VEN
				dbo.ED_IEV_PAT_INFO
				dbo.ED_IEV_EVENT_INFO
				dbo.ED_EVENT_TMPL_INFO
				dbo.ED_LEV_EVENT_INFO
				dbo.ZC_EVT_TMPLT_CLASS
				dbo.HNO_NOTE_TEXT
				dbo.CL_PLC

  Temp tables :
                #ClinicPatient
				#ScheduledAppointmentNote
				#EncounterSmartDataElement
				#ScheduledAppointmentInfectionStatus
                #ScheduledAppointment
				#IEV
                #ScheduledClinicAppointmentDetail
                #RptgTemp

      OUTPUTS:
                CLARITY_App_Dev.Stage.iQueue_Clinics_Status_Board_Extract
----------------------------------------------------------------------------------------------------------------------------------------
MODS:     11/05/2019--TMB-- Create new stored procedure
		  12/13/2019--TMB-- Edit reporting period, convert commas in enumerated strings
*****************************************************************************************************************************************/

  SET NOCOUNT ON;

---------------------------------------------------
---Default date range is the prior three days and the current date
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
          -- Current date
          SET @EndDate = CAST(@CurrDate AS SMALLDATETIME)
          + CAST(CAST('23:59:59' AS TIME) AS SMALLDATETIME);
      END;
----------------------------------------------------

  -- Create temp table #ClinicPatient with PAT_ID and Appt date

  SELECT DISTINCT
     pa.PAT_ID
	,pa.PAT_ENC_CSN_ID
    ,CAST(pa.[APPT_DTTM] AS DATE) AS [Appt date]
	,pt.PAT_MRN_ID
	,pt.LANGUAGE_C
  INTO #ClinicPatient
  FROM [CLARITY].[dbo].[V_SCHED_APPT] AS pa
  LEFT OUTER JOIN [CLARITY].[dbo].[PATIENT] AS pt
  ON pt.PAT_ID = pa.PAT_ID
  WHERE
  (pa.[APPT_DTTM] >= @StartDate AND pa.[APPT_DTTM] <= @EndDate)
   AND ((pa.DEPARTMENT_ID IN (10210001 -- ECCC HEM ONC EAST
                             ,10210002 -- ECCC HEM ONC WEST
                             ,10210030 -- ECCC NEURO WEST
							 ,10242051 -- UVPC DIGESTIVE HEALTH
							 ,10243003 -- UVHE DIGESTIVE HEALTH
							 ,10243087 -- UVHE SURG DIGESTIVE HL
							 ,10244023 -- UVWC MED GI CL
							 )
	    )
		--OR
		--(pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		--               ,'28954' -- CROPLEY, THOMAS
		--			   ,'89921' -- ISHARWAL, SUMIT
		--			   ,'29044' -- KRUPSKI, TRACEY
		--			   ,'56655' -- MAITLAND, HILLARY S
		--			   ,'29690' -- SHAFFREY, MARK
		--			   )
		--)
	   )


  -- Create index for temp table #ClinicPatient

  CREATE UNIQUE CLUSTERED INDEX IX_ClinicPaitent ON #ClinicPatient ([PAT_ID], [PAT_ENC_CSN_ID], [Appt date])
  CREATE NONCLUSTERED INDEX IX_ClinicPaitent2 ON #ClinicPatient ([PAT_ID], [Appt date])

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

  -- Create temp table #EncounterSmartDataElement

  SELECT SMRTDTA_ELEM_DATA.CONTACT_SERIAL_NUM
       , SMRTDTA_ELEM_VALUE.SMRTDTA_ELEM_VALUE
	   , SMRTDTA_ELEM_VALUE.LINE
  INTO #EncounterSmartDataElement
  FROM (SELECT DISTINCT
			PAT_ENC_CSN_ID
        FROM #ClinicPatient) ClinicPatient
  INNER JOIN
  (
  SELECT HLV_ID,
         ELEMENT_ID,
         CONTEXT_NAME,
         CONTACT_SERIAL_NUM
  FROM CLARITY.dbo.SMRTDTA_ELEM_DATA
  WHERE CONTEXT_NAME = 'ENCOUNTER'
  AND ELEMENT_ID = 'UVA#028' -- Visit Flow Comments
  ) SMRTDTA_ELEM_DATA
  ON ClinicPatient.PAT_ENC_CSN_ID = SMRTDTA_ELEM_DATA.CONTACT_SERIAL_NUM
  LEFT OUTER JOIN
  (
  SELECT HLV_ID
       , SMRTDTA_ELEM_VALUE
       , LINE
  FROM CLARITY.dbo.SMRTDTA_ELEM_VALUE
  WHERE LEN(SMRTDTA_ELEM_VALUE.SMRTDTA_ELEM_VALUE) > 0
  ) SMRTDTA_ELEM_VALUE
  ON SMRTDTA_ELEM_VALUE.HLV_ID = SMRTDTA_ELEM_DATA.HLV_ID
  ORDER BY SMRTDTA_ELEM_DATA.CONTACT_SERIAL_NUM
         , SMRTDTA_ELEM_VALUE.LINE

  -- Create index for temp table #EncounterSmartDataElement

  CREATE UNIQUE CLUSTERED INDEX IX_EncounterSmartDataElement ON #EncounterSmartDataElement (CONTACT_SERIAL_NUM, LINE)

  -- Create temp table #ScheduledAppointmentInfectionStatus

  SELECT hspi.PAT_ID
       , ClinicPatient.[Appt date]
       , hspi.INFECTION_C
	   , hspi.LINE
	   , hspi.INF_ADD_PL_TIME
	   , zinf.NAME AS INFECTION_NAME
  INTO #ScheduledAppointmentInfectionStatus
  FROM (SELECT DISTINCT
			PAT_ID
		  , [Appt date]
        FROM #ClinicPatient) ClinicPatient
  INNER JOIN CLARITY.dbo.HSP_INFECTION_PL AS hspi WITH(NOLOCK) ON ClinicPatient.PAT_ID = hspi.PAT_ID
  LEFT JOIN CLARITY.dbo.ZC_INFECTION AS zinf WITH(NOLOCK) ON hspi.INFECTION_C = zinf.INFECTION_C
  WHERE hspi.INF_ADD_PL_TIME IS NOT NULL
  AND hspi.INF_RSV_PL_TIME IS NULL
  AND (ClinicPatient.[Appt date] >= CAST(hspi.INF_ADD_PL_TIME AS DATE))
  ORDER BY hspi.PAT_ID
         , hspi.INF_ADD_PL_USER_ID
         , hspi.LINE

  -- Create index for temp table #ScheduledAppointmentInfectionStatus

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointmentInfectionStatus ON #ScheduledAppointmentInfectionStatus (PAT_ID, [Appt date], LINE)

  -- Create temp table #ScheduledAppointment

  SELECT
     pa.PAT_ID
    ,CAST(pa.[APPT_DTTM]           AS DATE)          AS [Appointment Date]
    ,CAST(pa.[APPT_DTTM]           AS SMALLDATETIME) AS [APPT_DTTM]
    ,pa.PAT_ENC_CSN_ID                               AS [PAT_ENC_CSN_ID_unhashed]
	,HASHBYTES('SHA2_256',CAST(enc.PAT_ENC_CSN_ID AS VARCHAR(18))) AS [PAT_ENC_CSN_ID]
    ,pt.PAT_MRN_ID									 AS [PAT_MRN_ID_unhashed]
	,HASHBYTES('SHA2_256',CAST(pt.PAT_MRN_ID AS VARCHAR(10))) AS [PAT_MRN_ID]
	,pa.DEPARTMENT_ID
    ,dep.DEPARTMENT_NAME                             AS [DEPARTMENT_NAME]
    ,pa.DEPT_SPECIALTY_NAME                          AS [DEPT_SPECIALTY_NAME]
	,apptnote.APPT_NOTES
	,sdevalue.SDE_VALUES
    ,pa.APPT_LENGTH                                  AS [APPT_LENGTH]
    ,pa.APPT_STATUS_C
    ,ZC_APPT_STATUS.NAME                             AS [APPT_STATUS_NAME]
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
	,pa.PROV_ID
	,CLARITY_SER.PROV_NAME
	,enct.NAME AS ENC_TYPE_NAME
	,DATEDIFF(
		MINUTE
		, F_SCHED_APPT.CHECKIN_DTTM
		, (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
			FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
						(CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
						(CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
						) AS Roomed_Cols(candidate_dttm))
		) AS TIME_TO_ROOM_MINUTES,
		DATEDIFF(
		MINUTE
		, (SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
			FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
						(CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
						(CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
				) AS Roomed_Cols(candidate_dttm))
		, (CASE WHEN F_SCHED_APPT.visit_end_dttm >= 
						(SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
						FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
									(CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
									(CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
								) AS Roomed_Cols(candidate_dttm))
						AND (F_SCHED_APPT.checkout_dttm IS NULL OR F_SCHED_APPT.visit_end_dttm <= F_SCHED_APPT.checkout_dttm)
						THEN F_SCHED_APPT.visit_end_dttm
				WHEN F_SCHED_APPT.checkout_dttm >= 
						(SELECT MIN(candidate_dttm) --select the earliest of three roomed timestamps occurring after check-in
							FROM (VALUES(CASE WHEN F_SCHED_APPT.arvl_list_remove_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.arvl_list_remove_dttm ELSE NULL END),
										(CASE WHEN F_SCHED_APPT.roomed_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.roomed_dttm ELSE NULL END), 
										(CASE WHEN F_SCHED_APPT.first_room_assign_dttm >= F_SCHED_APPT.checkin_dttm THEN F_SCHED_APPT.first_room_assign_dttm ELSE NULL END)
								) AS Roomed_Cols(candidate_dttm))
							AND (F_SCHED_APPT.visit_end_dttm IS NULL OR F_SCHED_APPT.checkout_dttm < F_SCHED_APPT.visit_end_dttm)
						THEN F_SCHED_APPT.checkout_dttm
				ELSE NULL
			END)
		) AS TIME_IN_ROOM_MINUTES,
		DATEDIFF(
			MINUTE
		, F_SCHED_APPT.CHECKIN_DTTM
		, CASE 
			WHEN F_SCHED_APPT.VISIT_END_DTTM >= F_SCHED_APPT.CHECKIN_DTTM
					AND (F_SCHED_APPT.CHECKOUT_DTTM IS NULL 
					OR F_SCHED_APPT.CHECKOUT_DTTM < F_SCHED_APPT.CHECKIN_DTTM
					OR F_SCHED_APPT.VISIT_END_DTTM <= F_SCHED_APPT.CHECKOUT_DTTM)
					THEN F_SCHED_APPT.VISIT_END_DTTM
			WHEN F_SCHED_APPT.CHECKOUT_DTTM >= F_SCHED_APPT.CHECKIN_DTTM
					THEN F_SCHED_APPT.CHECKOUT_DTTM
			ELSE NULL
		  END) AS CYCLE_TIME_MINUTES
		, ptinfst.INFECTION_NAMES
		, lng.NAME AS PRIMARY_LANGUAGE
		, enc.INTERPRETER_NEED_YN
		, enc.INTRP_ASSIGNMENT_C
		, enc.ASGND_INTERP_TYPE_C
		, enc.INTERPRETER_VEND_C
		, intrpven.NAME 'Interpreter Vendor'
		, intrp.NAME 'Interpreter Assignment'
		, intrptyp.NAME 'Interpreter type'

  INTO #ScheduledAppointment
  FROM [CLARITY].[dbo].[V_SCHED_APPT]     AS pa
  INNER JOIN
  (
  SELECT DISTINCT
	PAT_ID
  , [Appt date]
  , LANGUAGE_C
  FROM #ClinicPatient
  ) cp
  ON pa.PAT_ID = cp.PAT_ID AND CAST(pa.[APPT_DTTM] AS DATE) = cp.[Appt Date]
  LEFT OUTER JOIN [CLARITY].[dbo].[F_SCHED_APPT]	AS F_SCHED_APPT	ON F_SCHED_APPT.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.PAT_ENC     AS enc   ON enc.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS dep   ON pa.DEPARTMENT_ID   = dep.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS CLARITY_SER   ON pa.PROV_ID = CLARITY_SER.PROV_ID
  LEFT OUTER JOIN (SELECT DISTINCT
                          note.PAT_ENC_CSN_ID
                        , (SELECT COALESCE(MAX(notet.APPT_NOTE),'')  + '|' AS [text()]
		                   FROM #ScheduledAppointmentNote notet
						   WHERE notet.PAT_ENC_CSN_ID = note.PAT_ENC_CSN_ID
		                   GROUP BY notet.PAT_ENC_CSN_ID
				                  , notet.LINE
	                       FOR XML PATH ('')) AS APPT_NOTES
                   FROM #ScheduledAppointmentNote AS note)	AS apptnote	ON apptnote.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN (SELECT DISTINCT
                          sde.CONTACT_SERIAL_NUM
                        , (SELECT COALESCE(MAX(sdet.SMRTDTA_ELEM_VALUE),'')  + '|' AS [text()]
		                   FROM #EncounterSmartDataElement sdet
						   WHERE sdet.CONTACT_SERIAL_NUM = sde.CONTACT_SERIAL_NUM
		                   GROUP BY sdet.CONTACT_SERIAL_NUM
				                  , sdet.LINE
	                       FOR XML PATH ('')) AS SDE_VALUES
                   FROM #EncounterSmartDataElement AS sde)	AS sdevalue	ON sdevalue.CONTACT_SERIAL_NUM = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_STATUS	AS ZC_APPT_STATUS	ON ZC_APPT_STATUS.APPT_STATUS_C = enc.APPT_STATUS_C
  LEFT OUTER JOIN [CLARITY].[dbo].[PATIENT] AS pt	ON pt.PAT_ID = pa.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE	AS enct	ON enct.DISP_ENC_TYPE_C = enc.ENC_TYPE_C
  LEFT OUTER JOIN (SELECT DISTINCT
                          inf.PAT_ID
						, inf.[Appt date]
                        , (SELECT COALESCE(MAX(infst.INFECTION_NAME),'')  + '|' AS [text()]
		                   FROM #ScheduledAppointmentInfectionStatus infst
						   WHERE infst.PAT_ID = inf.PAT_ID
						   AND infst.[Appt date] = inf.[Appt date]
		                   GROUP BY infst.PAT_ID
						          , infst.[Appt date]
								  , infst.LINE
	                       FOR XML PATH ('')) AS INFECTION_NAMES
                   FROM #ScheduledAppointmentInfectionStatus AS inf)	AS ptinfst	ON ptinfst.PAT_ID = enc.PAT_ID AND ptinfst.[Appt date] = cp.[Appt date]
  LEFT OUTER JOIN CLARITY.dbo.ZC_LANGUAGE AS lng			ON lng.LANGUAGE_C = cp.LANGUAGE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_INTRP_ASSIGNMEN intrp		ON enc.INTRP_ASSIGNMENT_C=intrp.INTRP_ASSIGNMEN_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_ASGND_INTERP_TY intrptyp	ON enc.ASGND_INTERP_TYPE_C=intrptyp.ASGND_INTERP_TY_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_INTERPRETER_VEN intrpven	ON enc.INTERPRETER_VEND_C=intrpven.INTERPRETER_VEN_C

  WHERE ((pa.DEPARTMENT_ID IN (10210001 -- ECCC HEM ONC EAST
                             ,10210002 -- ECCC HEM ONC WEST
                             ,10210030 -- ECCC NEURO WEST
							 ,10242051 -- UVPC DIGESTIVE HEALTH
							 ,10243003 -- UVHE DIGESTIVE HEALTH
							 ,10243087 -- UVHE SURG DIGESTIVE HL
							 ,10244023 -- UVWC MED GI CL
							 )
	    )
		--OR
		--(pa.PROV_ID IN ('47947' -- ASTHAGIRI, ASHOK
		--               ,'28954' -- CROPLEY, THOMAS
		--			   ,'89921' -- ISHARWAL, SUMIT
		--			   ,'29044' -- KRUPSKI, TRACEY
		--			   ,'56655' -- MAITLAND, HILLARY S
		--			   ,'29690' -- SHAFFREY, MARK
		--			   )
		--)
	   )

  -- Create index for temp table #ScheduledAppointment

  CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointment ON #ScheduledAppointment ([PAT_ID], PAT_ENC_CSN_ID_unhashed, APPT_DTTM)

--Get Event Details for sample

SELECT
      ievpat.       PAT_ID
    , ievpat.       PAT_ENC_CSN_ID
    , ievpat.       TYPE_ID             AS TYPE_ID_pat                      -- Type of event template, joins to template
    , ievpat.       EVENT_ID            AS EVENT_ID_pat

    , ievevent.     EVENT_TYPE          AS EVENT_TYPE_iev                   -- Also joins to template	ED_EVENT_TMPL_INFO (RECORD_ID)
    , ievevent.     EVENT_ID            AS EVENT_ID_iev						-- ED_IEV_PAT_INFO, EVENT_CNCT_INFO, EVENT, ED_EVENT_HISTORY
    , ievevent.     EVENT_DISPLAY_NAME  AS EVENT_DISPLAY_NAME_iev
    , ievevent.     LINE                AS LINE_iev                
    , ievevent.     EVENT_TIME          AS EVENT_TIME_iev
	, ievevent.		EVENT_CMT			AS EVENT_CMT_iev
	, ievevent.		EVENT_DEPT_ID		AS EVENT_DEPT_ID_iev				-- CLARITY_DEP (DEPARTMENT_ID)
	, ievevent.		EVENT_KEY			AS EVENT_KEY_iev
	, ievevent.		EVENT_NOTE_ID		AS EVENT_NOTE_ID_iev				-- HNO_NOTE_TEXT, HNO_INFO, HNO_INFO_2 (NOTE_ID)
	, ievevent.		EVENT_PROV_ID		AS EVENT_PROV_ID_iev				-- CLARTIY_SER (PROV_ID)
	, ievevent.		LOCATION_ID			AS LOCATION_ID_iev					-- CL_PLC (LOCATION_EVNT_ID)

    , templt_iev.   RECORD_ID           AS RECORD_ID_iev
    , templt_iev.   RECORD_NAME         AS RECORD_NAME_iev
    , templt_iev.   EVENT_NAME          AS EVENT_NAME_iev

    , levclass_iev. EVT_TMPLT_CLASS_C   AS EVT_TMPLT_CLASS_C_iev
    , zcclass_iev.  NAME                AS EVT_TMPLT_CLASS_C_name_iev

    , templt_pat.   RECORD_ID           AS RECORD_ID_pat
    , templt_pat.   RECORD_NAME         AS RECORD_NAME_pat
    , templt_pat.   EVENT_NAME          AS EVENT_NAME_pat

    , levclass_pat. EVT_TMPLT_CLASS_C   AS EVT_TMPLT_CLASS_C_pat
    , zcclass_pat.  NAME                AS EVT_TMPLT_CLASS_C_name_pat
	, ievpat.		DEPT_EVENT_DEP_ID
	, dep.			DEPARTMENT_NAME
	, note.			LINE				AS NOTE_LINE
	, note.			NOTE_TEXT			AS NOTE_TEXT
	, ser.			PROV_ID				AS EVENT_PROV_ID
	, ser.			PROV_NAME			AS EVENT_PROV_NAME
	, plc.			COMMENTS			AS LOCATION_COMMENTS

INTO #IEV                                       
        
FROM  CLARITY.dbo.ED_IEV_PAT_INFO       ievpat
    LEFT JOIN CLARITY.dbo.ED_IEV_EVENT_INFO     ievevent            ON  ievpat.EVENT_ID     =   ievevent.EVENT_ID            
    LEFT JOIN CLARITY.dbo.ED_EVENT_TMPL_INFO    templt_iev          ON                          ievevent.EVENT_TYPE         =  templt_iev.RECORD_ID  --IEV.30
    LEFT JOIN CLARITY.dbo.ED_LEV_EVENT_INFO		levclass_iev        ON                                                         templt_iev.RECORD_ID     =   levclass_iev.RECORD_ID
	LEFT JOIN CLARITY.dbo.ZC_EVT_TMPLT_CLASS	zcclass_iev		    ON                                                                                      levclass_iev.EVT_TMPLT_CLASS_C	= zcclass_iev.EVT_TMPLT_CLASS_C
    LEFT JOIN CLARITY.dbo.ED_EVENT_TMPL_INFO    templt_pat          ON  ievpat.TYPE_ID                                      =  templt_pat.RECORD_ID  --IEV.31
    LEFT JOIN CLARITY.dbo.ED_LEV_EVENT_INFO		levclass_pat        ON                                                         templt_pat.RECORD_ID     =   levclass_pat.RECORD_ID
    LEFT JOIN CLARITY.dbo.ZC_EVT_TMPLT_CLASS	zcclass_pat	        ON                                                                                      levclass_pat.EVT_TMPLT_CLASS_C	= zcclass_pat.EVT_TMPLT_CLASS_C
	LEFT JOIN CLARITY.dbo.CLARITY_DEP		dep					ON	dep.DEPARTMENT_ID	=	ievpat.DEPT_EVENT_DEP_ID
	LEFT JOIN CLARITY.dbo.HNO_NOTE_TEXT		note				ON  note.NOTE_ID		=	ievevent.EVENT_NOTE_ID
	LEFT JOIN CLARITY.dbo.CLARITY_SER		ser					ON ser.PROV_ID          =	ievevent.EVENT_PROV_ID
	LEFT JOIN CLARITY.dbo.CL_PLC			plc					ON plc.LOCATION_EVNT_ID =	ievevent.LOCATION_ID
   
    INNER JOIN
	(
	SELECT DISTINCT
		PAT_ENC_CSN_ID_unhashed
	FROM #ScheduledAppointment
	) csns                ON ievpat.PAT_ENC_CSN_ID = csns.PAT_ENC_CSN_ID_unhashed
       
WHERE 1=1
AND ievevent.EVENT_TYPE IN
(
'1400000222'
,'2104200001'
,'2104200002'
,'1400000227'
,'1400000225'
,'1400000234'
,'57043'
,'1400000226'
,'1400000233'
,'1400000224'
,'1400000237'
,'1400000238'
,'2104500001'
,'1400000230'
,'1400000228'
,'1400000229'
,'1400000235'
,'1400000236'
,'1400000220'
,'604'
)

  -- Create temp table #ScheduledClinicAppointmentDetail

  SELECT
     pa.PAT_MRN_ID
    ,pa.PAT_ENC_CSN_ID
	,pa.PAT_ENC_CSN_ID_unhashed
    ,pa.DEPARTMENT_NAME
	,pa.PROV_NAME
	,iev.EVENT_DISPLAY_NAME_iev
	,pa.INFECTION_NAMES
	,pa.APPT_NOTES
	,iev.EVENT_TIME_iev
	,pa.SDE_VALUES
	,pa.PRIMARY_LANGUAGE
	,pa.[Interpreter Assignment]
	,pa.[Interpreter type]
	,pa.[Interpreter Vendor]
	,pa.APPT_STATUS_NAME
	,pa.APPT_DTTM
	,pa.APPT_LENGTH
	,pa.ENC_TYPE_NAME
	,pa.TIME_TO_ROOM_MINUTES
	,pa.TIME_IN_ROOM_MINUTES
	,pa.CYCLE_TIME_MINUTES
  INTO #ScheduledClinicAppointmentDetail
  FROM #ScheduledAppointment AS pa
  LEFT OUTER JOIN #IEV iev
  ON iev.PAT_ENC_CSN_ID = pa.PAT_ENC_CSN_ID_unhashed

  -- Create temp table #RptgTemp

  SELECT
     PAT_MRN_ID
    ,PAT_ENC_CSN_ID
	,PAT_ENC_CSN_ID_unhashed
    ,DEPARTMENT_NAME
	,PROV_NAME
	,EVENT_DISPLAY_NAME_iev
	,INFECTION_NAMES
	,APPT_NOTES
	,EVENT_TIME_iev
	,SDE_VALUES
	,PRIMARY_LANGUAGE
	,[Interpreter Assignment]
	,[Interpreter type]
	,[Interpreter Vendor]
	,APPT_STATUS_NAME
	,APPT_DTTM
	,APPT_LENGTH
	,ENC_TYPE_NAME
	,TIME_TO_ROOM_MINUTES
	,TIME_IN_ROOM_MINUTES
	,CYCLE_TIME_MINUTES
    ,'Rptg.uspSrc_iQueue_Clinics_Status_Board_Daily' AS [ETL_guid]
    ,GETDATE() AS Load_Dte
  INTO #RptgTemp FROM
   (
    SELECT
     pa.PAT_MRN_ID
    ,pa.PAT_ENC_CSN_ID
	,pa.PAT_ENC_CSN_ID_unhashed
    ,pa.DEPARTMENT_NAME
	,pa.PROV_NAME
	,pa.EVENT_DISPLAY_NAME_iev
	,pa.INFECTION_NAMES
	,pa.APPT_NOTES
	,pa.EVENT_TIME_iev
	,pa.SDE_VALUES
	,pa.PRIMARY_LANGUAGE
	,pa.[Interpreter Assignment]
	,pa.[Interpreter type]
	,pa.[Interpreter Vendor]
	,pa.APPT_STATUS_NAME
	,pa.APPT_DTTM
	,pa.APPT_LENGTH
	,pa.ENC_TYPE_NAME
	,pa.TIME_TO_ROOM_MINUTES
	,pa.TIME_IN_ROOM_MINUTES
	,pa.CYCLE_TIME_MINUTES
    FROM #ScheduledClinicAppointmentDetail AS pa
   ) A

  -- Put contents of temp table #RptgTemp into db table

  INSERT INTO Stage.iQueue_Clinics_Status_Board_Extract
  (
      PAT_ENC_CSN_ID_unhashed,
      PAT_ENC_CSN_ID,
      PAT_MRN_ID,
      DEPARTMENT_NAME,
      PROV_NAME,
      EVENT_DISPLAY_NAME,
      INFECTION_STATUS,
      APPT_NOTES,
      EVENT_DTTM,
      VISIT_FLOW_COMMENTS,
      PRIMARY_LANGUAGE,
      INTERPRETER_ASSIGNMENT,
      INTERPRETER_TYPE,
      INTERPRETER_VENDOR,
      APPT_STATUS_NAME,
      APPT_DTTM,
      APPT_LENGTH,
      ENC_TYPE_NAME,
      TIME_TO_ROOM_MINUTES,
      TIME_IN_ROOM_MINUTES,
      CYCLE_TIME_MINUTES,
      ETL_guid,
      Load_Dte
  )

  SELECT
    PAT_ENC_CSN_ID_unhashed
   ,ISNULL(CONVERT(VARCHAR(256),[PAT_ENC_CSN_ID],2),'')					AS [PAT_ENC_CSN_ID]
   ,ISNULL(CONVERT(VARCHAR(256),[PAT_MRN_ID],2),'')						AS [PAT_MRN_ID]
   ,ISNULL(CONVERT(VARCHAR(254),[DEPARTMENT_NAME]),'')					AS [DEPARTMENT_NAME]
   ,ISNULL(CONVERT(VARCHAR(200),[PROV_NAME]),'')						AS [PROV_NAME]
   ,ISNULL(CONVERT(VARCHAR(254),[EVENT_DISPLAY_NAME_iev]),'')			AS [EVENT_DISPLAY_NAME]
   ,CASE
      WHEN CONVERT(VARCHAR(1200),LEFT([INFECTION_NAMES],LEN([INFECTION_NAMES])-1)) IS NULL THEN CAST(''  AS VARCHAR(1200))
      ELSE CAST(REPLACE(CONVERT(VARCHAR(1200),LEFT([INFECTION_NAMES],LEN([INFECTION_NAMES])-1)),',','^') AS VARCHAR(1200))
    END                                                                 AS [INFECTION_STATUS]
   ,CASE
      WHEN CONVERT(VARCHAR(1200),LEFT([APPT_NOTES],LEN([APPT_NOTES])-1)) IS NULL THEN CAST(''  AS VARCHAR(1200))
      ELSE CAST(REPLACE(CONVERT(VARCHAR(1200),LEFT([APPT_NOTES],LEN([APPT_NOTES])-1)),',','^') AS VARCHAR(1200))
    END                                                                 AS [APPT_NOTES]
   ,[EVENT_TIME_iev]													AS [EVENT_DTTM]
   ,CASE
      WHEN CONVERT(VARCHAR(1200),LEFT([SDE_VALUES],LEN([SDE_VALUES])-1)) IS NULL THEN CAST(''  AS VARCHAR(1200))
      ELSE CAST(REPLACE(CONVERT(VARCHAR(1200),LEFT([SDE_VALUES],LEN([SDE_VALUES])-1)),',','^') AS VARCHAR(1200))
    END                                                                 AS [VISIT_FLOW_COMMENTS]
   ,ISNULL(CONVERT(VARCHAR(200),[PRIMARY_LANGUAGE]),'')					AS [PRIMARY_LANGUAGE]
   ,ISNULL(CONVERT(VARCHAR(254),[Interpreter Assignment]),'')			AS [INTERPRETER_ASSIGNMENT]
   ,ISNULL(CONVERT(VARCHAR(254),[Interpreter type]),'')					AS [INTERPRETER_TYPE]
   ,ISNULL(CONVERT(VARCHAR(254),[Interpreter Vendor]),'')				AS [INTERPRETER_VENDOR]
   ,ISNULL(CONVERT(VARCHAR(254),[APPT_STATUS_NAME]),'')					AS [APPT_STATUS_NAME]
   ,ISNULL(CONVERT(VARCHAR(19),[APPT_DTTM],121),'')						AS [APPT_DTTM]
   ,ISNULL(CONVERT(VARCHAR(18),[APPT_LENGTH]),'')						AS [APPT_LENGTH]
   ,ISNULL(CONVERT(VARCHAR(254),[ENC_TYPE_NAME]),'')					AS [ENC_TYPE_NAME]
   ,ISNULL(CONVERT(VARCHAR(18),[TIME_TO_ROOM_MINUTES]),'')				AS [TIME_TO_ROOM_MINUTES]
   ,ISNULL(CONVERT(VARCHAR(18),[TIME_IN_ROOM_MINUTES]),'')				AS [TIME_IN_ROOM_MINUTES]
   ,ISNULL(CONVERT(VARCHAR(18),[CYCLE_TIME_MINUTES]),'')				AS [CYCLE_TIME_MINUTES]
   ,[ETL_guid]
   ,CONVERT(VARCHAR(19),[Load_Dte],121) AS [Load_Dte]
  FROM #RptgTemp
  ORDER BY APPT_DTTM

GO



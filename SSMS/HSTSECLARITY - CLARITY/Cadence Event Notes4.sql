USE CLARITY
-- LEV are Patient Events:      Event Template records store details about each event template that can be used to record an event in Hyperspace, including the event's name, abbreviation, and template class. Each template record defines the events that can be recorded and, unlike event records, is not patient-specific. Examples of template records include assigning a bed, placing an order, and printing a patient's After Visit Summary (AVS).
-- IEV are Appointment Events:  Events records store information about the events that are recorded for each patient, including the date and time at which each event was recorded, the user who recorded each event, and the event type of each event. Each type of event that can occur has a corresponding event template (LEV) record.


DROP TABLE IF EXISTS #CSNs

--Get Sample
--SELECT TOP 100 PAT_ENC_CSN_ID
SELECT PAT_ENC_CSN_ID
    INTO #CSNs
FROM V_SCHED_APPT 
WHERE   1=1
    --AND LOC_ID = '10354'
    --AND DEPARTMENT_ID = '10354014'
	AND DEPARTMENT_ID = '10210002'
    --AND PROV_ID = '8'
    --AND CONTACT_DATE BETWEEN '2018-12-17' AND '2018-12-17'
    AND CONTACT_DATE >= '2019-8-1'
    AND APPT_STATUS_C = 2
	--AND PAT_ENC_CSN_ID = 200020165768
	AND PAT_ENC_CSN_ID IN
(200019726833
,200020019768
,200020126123
,200020315755
,200019994407
)
;

--Get V_SCHED_APPT Details for sample
--SELECT vsa.* 
--FROM V_SCHED_APPT vsa
--    INNER JOIN #CSNs csn   ON vsa.PAT_ENC_CSN_ID = csn.PAT_ENC_CSN_ID
--ORDER BY CONTACT_DATE
--;

--Get Event Details for sample
WITH cte AS (
SELECT --TOP 100
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
    --, templt_iev.   DISPLAY_NAME        AS DISPLAY_NAME_iev

    , levclass_iev. EVT_TMPLT_CLASS_C   AS EVT_TMPLT_CLASS_C_iev
    , zcclass_iev.  NAME                AS EVT_TMPLT_CLASS_C_name_iev

    , templt_pat.   RECORD_ID           AS RECORD_ID_pat
    , templt_pat.   RECORD_NAME         AS RECORD_NAME_pat
    , templt_pat.   EVENT_NAME          AS EVENT_NAME_pat
    --, templt_pat.   DISPLAY_NAME        AS DISPLAY_NAME_pat

    , levclass_pat. EVT_TMPLT_CLASS_C   AS EVT_TMPLT_CLASS_C_pat
    , zcclass_pat.  NAME                AS EVT_TMPLT_CLASS_C_name_pat
	, ievpat.		DEPT_EVENT_DEP_ID
	, dep.			DEPARTMENT_NAME
	, note.			LINE				AS NOTE_LINE
	, note.			NOTE_TEXT			AS NOTE_TEXT
	, ser.			PROV_ID				AS EVENT_PROV_ID
	, ser.			PROV_NAME			AS EVENT_PROV_NAME
	, plc.			COMMENTS			AS LOCATION_COMMENTS
                                       
        
FROM  ED_IEV_PAT_INFO       ievpat
    LEFT JOIN ED_IEV_EVENT_INFO     ievevent            ON  ievpat.EVENT_ID     =   ievevent.EVENT_ID            
    LEFT JOIN ED_EVENT_TMPL_INFO    templt_iev          ON                          ievevent.EVENT_TYPE         =  templt_iev.RECORD_ID  --IEV.30
    LEFT JOIN ED_LEV_EVENT_INFO		levclass_iev        ON                                                         templt_iev.RECORD_ID     =   levclass_iev.RECORD_ID
	LEFT JOIN ZC_EVT_TMPLT_CLASS	zcclass_iev		    ON                                                                                      levclass_iev.EVT_TMPLT_CLASS_C	= zcclass_iev.EVT_TMPLT_CLASS_C
    LEFT JOIN ED_EVENT_TMPL_INFO    templt_pat          ON  ievpat.TYPE_ID                                      =  templt_pat.RECORD_ID  --IEV.31
    LEFT JOIN ED_LEV_EVENT_INFO		levclass_pat        ON                                                         templt_pat.RECORD_ID     =   levclass_pat.RECORD_ID
    LEFT JOIN ZC_EVT_TMPLT_CLASS	zcclass_pat	        ON                                                                                      levclass_pat.EVT_TMPLT_CLASS_C	= zcclass_pat.EVT_TMPLT_CLASS_C
	LEFT JOIN dbo.CLARITY_DEP		dep					ON	dep.DEPARTMENT_ID	=	ievpat.DEPT_EVENT_DEP_ID
	LEFT JOIN dbo.HNO_NOTE_TEXT		note				ON  note.NOTE_ID		=	ievevent.EVENT_NOTE_ID
	LEFT JOIN dbo.CLARITY_SER		ser					ON ser.PROV_ID          =	ievevent.EVENT_PROV_ID
	LEFT JOIN dbo.CL_PLC			plc					ON plc.LOCATION_EVNT_ID =	ievevent.LOCATION_ID
   
    INNER JOIN #CSNs                csns                ON ievpat.PAT_ENC_CSN_ID = csns.PAT_ENC_CSN_ID
       
WHERE 1=1
--AND levclass_iev.EVT_TMPLT_CLASS_C IN (30, 56009)     -- For Type='Physician In'
--AND ievpat.PAT_ENC_CSN_ID = '200003589799'
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

)

--SELECT DISTINCT
--      ievevent.     EVENT_TYPE          AS EVENT_TYPE_iev                   -- Also joins to template
--    --, ievevent.     EVENT_ID            AS EVENT_ID_iev
--    , ievevent.     EVENT_DISPLAY_NAME  AS EVENT_DISPLAY_NAME_iev
--    --, ievevent.     LINE                AS LINE_iev                
--    --, ievevent.     EVENT_TIME          AS EVENT_TIME_iev          

--    --, templt_iev.   RECORD_ID           AS RECORD_ID_iev
--    --, templt_iev.   RECORD_NAME         AS RECORD_NAME_iev
--    --, templt_iev.   EVENT_NAME          AS EVENT_NAME_iev
--    --, templt_iev.   DISPLAY_NAME        AS DISPLAY_NAME_iev

--    , levclass_iev. EVT_TMPLT_CLASS_C   AS EVT_TMPLT_CLASS_C_iev
--    , zcclass_iev.  NAME                AS EVT_TMPLT_CLASS_C_name_iev
                                         
--FROM  ED_IEV_EVENT_INFO     ievevent            
--    LEFT JOIN ED_EVENT_TMPL_INFO    templt_iev          ON                          ievevent.EVENT_TYPE         =  templt_iev.RECORD_ID  --IEV.30
--    LEFT JOIN ED_LEV_EVENT_INFO		levclass_iev        ON                                                         templt_iev.RECORD_ID     =   levclass_iev.RECORD_ID
--	LEFT JOIN ZC_EVT_TMPLT_CLASS	zcclass_iev		    ON                                                                                      levclass_iev.EVT_TMPLT_CLASS_C	= zcclass_iev.EVT_TMPLT_CLASS_C
--ORDER BY ievevent.EVENT_DISPLAY_NAME

--------------------    Query 1
--/*
SELECT 
        PAT_ID
    ,   PAT_ENC_CSN_ID
    ,   TYPE_ID_pat
    ,   EVENT_ID_pat
    ,   EVT_TMPLT_CLASS_C_iev
    ,   EVT_TMPLT_CLASS_C_name_iev
    ,   EVENT_TYPE_iev
    ,   EVENT_DISPLAY_NAME_iev
    ,   LINE_iev
    ,   EVENT_TIME_iev
	,	DEPARTMENT_NAME
	,	NOTE_LINE
	,	NOTE_TEXT
	,	EVENT_PROV_ID
	,	EVENT_PROV_NAME
	,	LOCATION_COMMENTS
FROM cte
ORDER BY
        PAT_ID
    ,   PAT_ENC_CSN_ID
    ,   EVENT_TIME_iev
    ,   EVENT_ID_pat
    ,   LINE_iev
--SELECT DISTINCT
--        --TYPE_ID_pat
--    --,   EVENT_ID_pat
--        EVT_TMPLT_CLASS_C_iev
--    ,   EVT_TMPLT_CLASS_C_name_iev
--    ,   EVENT_TYPE_iev
--    ,   EVENT_DISPLAY_NAME_iev
--    --,   LINE_iev
--    --,   EVENT_TIME_iev
--SELECT DISTINCT
--        DEPT_EVENT_DEP_ID
--	,   DEPARTMENT_NAME
--	FROM cte
--ORDER BY
--        EVT_TMPLT_CLASS_C_iev
--    ,   EVT_TMPLT_CLASS_C_name_iev
--    ,   EVENT_TYPE_iev
--    ,   EVENT_DISPLAY_NAME_iev
--ORDER BY
--        EVENT_DISPLAY_NAME_iev
--ORDER BY
--        DEPT_EVENT_DEP_ID
--*/
;
/*
SELECT  vav.*
FROM V_AVAILABILITY vav
    INNER JOIN #CSNs        csns    ON vav.PAT_ENC_CSN_ID = csns.PAT_ENC_CSN_ID

    UNION ALL

SELECT  vav.*
FROM V_AVAILABILITY vav
WHERE 1=1
AND SLOT_DATE IN (SELECT DISTINCT vsa.CONTACT_DATE FROM #CSNs csns LEFT JOIN V_SCHED_APPT vsa ON csns.PAT_ENC_CSN_ID = vsa.PAT_ENC_CSN_ID) 
AND APPT_NUMBER = 0
*/
; DROP TABLE IF EXISTS #CSNs;

/*
--------------------    Query 2 
SELECT 'Event_Data' AS DataSet, PAT_ENC_CSN_ID, EVENT_DISPLAY_NAME, NULL AS vsa_TimeType, EVENT_TIME
FROM CTE

UNION ALL

SELECT 'Appt_Data', PAT_ENC_CSN_ID, NULL, TimeType, TimeDTTM
FROM 
(   SELECT V_SCHED_APPT.PAT_ENC_CSN_ID, APPT_DTTM, SIGNIN_DTTM, CHECKIN_DTTM, ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, NURSE_LEAVE_DTTM, PHYS_ENTER_DTTM, VISIT_END_DTTM, CHECKOUT_DTTM
    FROM V_SCHED_APPT
        INNER JOIN #CSNs        csns ON V_SCHED_APPT.PAT_ENC_CSN_ID = csns.PAT_ENC_CSN_ID
) AS Records
UNPIVOT
(   TimeDTTM FOR    TimeType IN (APPT_DTTM, SIGNIN_DTTM, CHECKIN_DTTM, ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, NURSE_LEAVE_DTTM, PHYS_ENTER_DTTM, VISIT_END_DTTM, CHECKOUT_DTTM)
) AS Up
*/

/*
--------------------    Query 3
SELECT COALESCE(CTE.PAT_ENC_CSN_ID, vsa.PAT_ENC_CSN_ID) AS CSN, EVENT_DISPLAY_NAME, vsa.TimeType, COALESCE(EVENT_TIME, vsa.TimeDTTM) AS TimeDTTM
FROM CTE
    FULL OUTER JOIN (   SELECT PAT_ENC_CSN_ID, TimeType, TimeDTTM
                        FROM 
                        (   SELECT V_SCHED_APPT.PAT_ENC_CSN_ID, APPT_DTTM, SIGNIN_DTTM, CHECKIN_DTTM, ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, NURSE_LEAVE_DTTM, PHYS_ENTER_DTTM, VISIT_END_DTTM, CHECKOUT_DTTM
                            FROM V_SCHED_APPT
                                INNER JOIN #CSNs        csns ON V_SCHED_APPT.PAT_ENC_CSN_ID = csns.PAT_ENC_CSN_ID
                        ) AS Records
                        UNPIVOT
                        (   TimeDTTM FOR    TimeType IN (APPT_DTTM, SIGNIN_DTTM, CHECKIN_DTTM, ARVL_LIST_REMOVE_DTTM, ROOMED_DTTM, NURSE_LEAVE_DTTM, PHYS_ENTER_DTTM, VISIT_END_DTTM, CHECKOUT_DTTM)
                        ) AS Up
                    )   vsa ON  cte.PAT_ENC_CSN_ID =  vsa.PAT_ENC_CSN_ID
                            AND cte.EVENT_TIME      = vsa.TimeDTTM
ORDER BY 1
*/

/*
select * from V_SCHED_EVENTS where PAT_ENC_CSN_ID='200008729122'
select * from V_SCHED_APPT where PAT_ENC_CSN_ID='200008729122'

SELECT * FROM CLARITY_DEP WHERE DEPARTMENT_NAME LIKE '%BB%'
SELECT * FROM CLARITY_LOC WHERE LOC_NAME LIKE '%BATTLE%'


select TOP 100 PAT_ENC_CSN_ID
FROM V_SCHED_APPT 
WHERE LOC_ID = '10354'
AND CONTACT_DATE BETWEEN '2018-11-01' AND '2018-11-30'

SELECT * FROM ED_IEV_PAT_INFO WHERE PAT_ENC_CSN_ID = '200003589226'                     -- Pat CSN to Event ID
SELECT * FROM ED_IEV_EVENT_INFO WHERE EVENT_ID IN ('26220316', '26219451', '26261247')  -- Event ID to Event Detail
SELECT * FROM ED_EVENT_TMPL_INFO WHERE RECORD_ID IN ('57021', '35000', '605')
  


-- Event Template LEV records
SELECT zcclass.NAME AS Template_Class_Name, levclass.EVT_TMPLT_CLASS_C,  lev.* 
FROM ED_EVENT_TMPL_INFO		lev
	LEFT JOIN ED_LEV_EVENT_INFO		levclass	ON lev.RECORD_ID				= levclass.RECORD_ID
	LEFT JOIN ZC_EVT_TMPLT_CLASS	zcclass		ON levclass.EVT_TMPLT_CLASS_C	= zcclass.EVT_TMPLT_CLASS_C
--WHERE lev.RECORD_ID = '1400000221'
WHERE levclass.EVT_TMPLT_CLASS_C IN ('30', '56009')
AND RECORD_NAME LIKE 'WAITING%'
ORDER BY levclass.EVT_TMPLT_CLASS_C, lev.RECORD_ID

SELECT * FROM ED_EVENT_TMPL_INFO	WHERE RECORD_ID = '1400000231'										https://datahandbook.epic.com/ClarityDictionary/Details?ver=8200&tblName=ED_EVENT_TMPL_INFO

SELECT * FROM ED_LEV_EVENT_INFO	WHERE RECORD_ID = '56009'	-- Event Templates							https://datahandbook.epic.com/ClarityDictionary/Details?ver=8200&tblName=ED_LEV_EVENT_INFO
SELECT * FROM ZC_EVT_TMPLT_CLASS	-- Event Template Classes category list		                        https://datahandbook.epic.com/ClarityDictionary/Details?ver=8200&tblName=ZC_EVT_TMPLT_CLASS

SELECT top 100 * FROM ED_IEV_EVENT_INFO	WHERE EVENT_TYPE = '56009'
SELECT top 100 * FROM ED_IEV_PAT_INFO	WHERE TYPE_ID = '56009' 

SELECT top 100 * FROM ED_IEV_EVENT_INFO	WHERE EVENT_ID = '26125549'
SELECT top 100 * FROM ED_IEV_PAT_INFO	WHERE EVENT_ID = '26125549'

INI Definitions:
LEV-Event Templates - Event Template records store details about each event template that can be used to record an event in Hyperspace, including the event's name, abbreviation, and template class. Each template record defines the events that can be recorded and, unlike event records, is not patient-specific. Examples of template records include assigning a bed, placing an order, and printing a patient's After Visit Summary (AVS).
IEV-Events - Events records store information about the events that are recorded for each patient, including the date and time at which each event was recorded, the user who recorded each event, and the event type of each event. Each type of event that can occur has a corresponding event template (LEV) record.
PLF-Patient Location Facility - This master file stores information related to patient locations, including contact number, location type, and whether it is private.
PLC-Patient Location - This master file stores patient location information. It is reserved for future development.

-- Get all LEV info
SELECT zcclass.NAME AS Template_Class_Name, levclass.EVT_TMPLT_CLASS_C,  lev.* 
FROM ED_EVENT_TMPL_INFO		lev
	LEFT JOIN ED_LEV_EVENT_INFO		levclass	ON lev.RECORD_ID				= levclass.RECORD_ID
	LEFT JOIN ZC_EVT_TMPLT_CLASS	zcclass		ON levclass.EVT_TMPLT_CLASS_C	= zcclass.EVT_TMPLT_CLASS_C
--WHERE lev.RECORD_ID = '1400000221'
WHERE 1=1
AND levclass.EVT_TMPLT_CLASS_C IN ('30') --, '56009')
--AND RECORD_NAME LIKE 'WAITING%'
OR lev.RECORD_ID IN ('600', '1400000220', '1400000222', '1400000224', '1400000225', '1400000226', '1400000227', '1400000228', '1400000229', '1400000230', '1400000233', '1400000234', '1400000235', '1400000236', '1400000237', '1400000238', '2104200001', '2104200002', '2104200004', '2104500001')

ORDER BY levclass.EVT_TMPLT_CLASS_C, lev.RECORD_ID

*/

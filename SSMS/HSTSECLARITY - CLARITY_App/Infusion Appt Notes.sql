USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROC [ETL].[uspSrc_Telemedicine_Appt_Notes]
--AS

/*  Proc to preload encounters for specific telemedicine appointment notes to a stage table for use
    in the subsequent Telemedicine Extract process 
	Query by Brian Costello
	Proc by Bryan Dunn
	04/14/2020

--------------------------------------------------------------------------------------------------------------------------
--MODS:       
--			04/17/2020	- BJC	-	Added date filters for contact dates >= 07-01-2017 (Epic Phase 2 go-live), 
									added distinct encounter filtering, added Appt_Note colume, 
--			04/23/2020	- BJC	-	Updated logic for Video / Audio
--			04/24/2020	- BJC	-	Updated logic for Video / Audio
--			05/19/2020	- BJC	-	Add Doxy to inclusion logic for Video
--			05/22/2020	- BJC	-	Updated logic for Video / Audio. Per L Oktay, no longer assign Video or Phone assigned based on Scheduling Notes
--          05/25/2020  - BDD   -   changed COMM_TYPE from null to blank string because target column in not null. Also added correct type cast
--			07/01/2020	- BJC	-	Include ALL appointmnet notes for Telemedicine Encounters per Kerry Cotter

--************************************************************************************************************************

*/	

SET NOCOUNT ON 

;WITH NOTES AS (
			SELECT
			FLATTENED.Encounter_CSN
			,FLATTENED.APPT_NOTE 
			
			FROM  (
						SELECT 
							PAT_ENC_CSN_ID			AS Encounter_CSN
							,APPT_NOTE= STUFF(
							 (SELECT '|' + N1.APPT_NOTE
							  FROM CLARITY..PAT_ENC_APPT_NOTES N1
							  							  WHERE N1.PAT_ENC_CSN_ID = N2.PAT_ENC_CSN_ID
							  FOR XML PATH (''))
							 , 1, 1, '') FROM CLARITY..PAT_ENC_APPT_NOTES N2
							 GROUP BY PAT_ENC_CSN_ID) FLATTENED
						--INNER JOIN (SELECT PAT_ENC_CSN_ID FROM CLARITY..PAT_ENC WHERE DEPARTMENT_ID = 10280005) AS INF ON FLATTENED.Encounter_CSN = INF.PAT_ENC_CSN_ID
						INNER JOIN (SELECT PAT_ENC_CSN_ID FROM CLARITY..PAT_ENC WHERE VISIT_PROV_ID = '1301225') AS INF ON FLATTENED.Encounter_CSN = INF.PAT_ENC_CSN_ID
				)


SELECT 
			NOTES.Encounter_CSN
			,CAST('' AS VARCHAR(25)) AS COMM_TYPE  -- per L Oktay do not assign Video or Phone assigned based on Scheduling Notes
			,CAST(NOTES.APPT_NOTE AS VARCHAR(255))	AS APPT_NOTE
			,V_SCHED_APPT.CONTACT_DATE
			,V_SCHED_APPT.APPT_STATUS_NAME			AS APPT_STATUS
			,V_SCHED_APPT.DEPARTMENT_ID
			,V_SCHED_APPT.DEPARTMENT_NAME
			,V_SCHED_APPT.PROV_NAME_WID
			,V_SCHED_APPT.PRC_NAME  VISIT_TYPE
			,V_PAT_ENC.ENC_TYPE_TITLE				AS ENC_TYPE



FROM NOTES

LEFT JOIN CLARITY..V_SCHED_APPT ON V_SCHED_APPT.PAT_ENC_CSN_ID=NOTES.Encounter_CSN
LEFT JOIN CLARITY..V_PAT_ENC ON V_PAT_ENC.PAT_ENC_CSN_ID=NOTES.Encounter_CSN

ORDER BY NOTES.APPT_NOTE


GO



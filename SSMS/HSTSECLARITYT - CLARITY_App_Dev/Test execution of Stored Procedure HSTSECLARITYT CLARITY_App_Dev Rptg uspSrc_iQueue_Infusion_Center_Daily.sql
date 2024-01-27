USE CLARITY_App_Dev
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @schema VARCHAR(100), @Table VARCHAR(100)

SET @schema = 'Stage'
SET @Table = 'iQueue_Infusion_Extract'

BEGIN
DECLARE @SQL NVARCHAR(500)

PRINT '---------  truncating table ' + @schema + '.' + @Table + ' -----------'	
SET @sql = 'TRUNCATE TABLE ' + @schema + '.' + @Table
--PRINT @SQL
BEGIN TRY

EXEC sp_executeSQL @SQL
IF @@ERROR = 0
PRINT '------------- ' + @schema + '.' + @Table + ' was truncated. ---------'
REVERT
END TRY

BEGIN CATCH

DECLARE @ErrorMessage NVARCHAR ( 4000 ), 
@ErrorNumber INT , 
@ErrorSeverity INT , 
@ErrorState INT , 
@ErrorLine INT , 
@ErrorProcedure NVARCHAR ( 200 ); 

SELECT @ErrorNumber = ERROR_NUMBER (), 
@ErrorSeverity = ERROR_SEVERITY (), 
@ErrorState = ERROR_STATE (), 
@ErrorLine = ERROR_LINE (), 
@ErrorProcedure = ISNULL ( ERROR_PROCEDURE (), '-' ); 
SELECT @ErrorMessage = N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
'Message: ' + ERROR_MESSAGE (); 

-- Raise an error: msg_str parameter of RAISERROR will contain 

-- the original error information. 

RAISERROR ( @ErrorMessage , @ErrorSeverity , 1 , 
@ErrorNumber , @ErrorSeverity , @ErrorState , @ErrorProcedure , 
@ErrorLine 
) 

END CATCH

END

DECLARE @RC INT
DECLARE @StartDate SMALLDATETIME = NULL
DECLARE @EndDate SMALLDATETIME = NULL

SET @StartDate = '7/1/2021 00:00:00'

-- TODO: Set parameter values here.

EXECUTE @RC = [Rptg].[uspSrc_iQueue_Infusion_Center_Daily] 
   @StartDate
  ,@EndDate

GO




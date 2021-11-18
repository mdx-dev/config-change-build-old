
CREATE OR ALTER PROCEDURE tix.ccb_ssp_CreateBuild (
    @Detail_CreateBuild tix.ccb_ut_BuildDetail READONLY
  , @BuildName varchar(100) = ''
  , @Ticket varchar(30) = NULL
  , @ImportType varchar(50)
  , @Notes varchar(255) = NULL
) AS

-- ==========================================================================================
-- Description: Recieves build detail records and then generates a record in tix.ccb_Build.
--              After the build table record is made, the BuildID from that record is then
--              used as the foreign key in tix.ccb_BuildDetail
-- Parameters:
--    @tix.ccb_ut_BuildDetail
--        EntityTypeID, EntityID, ProcedureID, SettingValueNew, NULL as SettingValueOLD
-- Returns:
--    New record in tix.ccb_Build
--    New records in tix.ccb_BuildDetail with the newly generated BuildID
-- ==========================================================================================

BEGIN
SET NOCOUNT ON;

DECLARE @BuildDetail_Insert tix.ccb_ut_BuildDetail
      , @buildID int -- The BuildID which will be generated for the build being created.
	  , @enviornment varchar(10); -- The enviornment in which in the build was created. Supports QA, UAT, SIM, and PRD.

---------------------------------------------------------------------------------------------------------------
-------------------------------------------- BUILD TABLE RECORD -----------------------------------------------
---------------------------------------------------------------------------------------------------------------

-- Captures the current enviornment to insert into the build table.
SET @enviornment = (SELECT CASE WHEN @@SERVERNAME = 'AWSVNHDBQA01'  AND DB_NAME() = 'CAV22'     THEN 'QA'
                                WHEN @@SERVERNAME = 'AWSVNHDBQA01'  AND DB_NAME() = 'CAV22_UAT' THEN 'UAT'
								WHEN @@SERVERNAME = 'AWSVNHDBLNP01' AND DB_NAME() = 'CAV22_SIM' THEN 'SIM'
								WHEN @@SERVERNAME = 'AWSVNHDBPRD03' AND DB_NAME() = 'CAV22'     THEN 'PRD'
								ELSE 'UNKN'
							END);

-- Creating a record in the build table for the new build
INSERT INTO tix.ccb_Build (BuildName, IsDeployed, DateLastDeployed, DateLastRolledBack, Ticket, ImportType, Enviornment, DateAdded, AddedBy, Notes)
VALUES (@BuildName, 0, NULL, NULL, @Ticket, @ImportType, @Enviornment, CURRENT_TIMESTAMP, SUSER_NAME(), @Notes);

-- Captures the newly generated BuildID foruse in the BuilDetail table
SET @buildID = SCOPE_IDENTITY();

-- If no Build Name is specified, the name assigned will be "Build {BuildID}"
IF @BuildName = '' OR @BuildName IS NULL
    UPDATE tix.ccb_Build
	   SET BuildName = CONCAT('Build ', CAST(@buildID as varchar))
	FROM tix.ccb_Build
	WHERE BuildID = @buildID

---------------------------------------------------------------------------------------------------------------
---------------------------------------- BUILD DETAIL TABLE RECORDS -------------------------------------------
---------------------------------------------------------------------------------------------------------------

CREATE TABLE #BuildDetailInsert (
    EntityTypeID           tinyint      NOT NULL
  , EntityID               bigint       NOT NULL
  , ProcedureID            bigint       NOT NULL
  , SettingID              smallint     NOT NULL
  , SettingValueNew        varchar(255) NOT NULL
  , SettingValueOld        varchar(255) NOT NULL
);

INSERT INTO #BuildDetailInsert (EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld)
EXEC tix.ccb_ssp_GetCurrentSetting @Detail_CreateBuild

-- Insert the current and new setting values, along with the newly generated BuildID to the BuildDetail table
INSERT INTO tix.ccb_BuildDetail (BuildID, EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld)
SELECT @buildID as BuildID
     , EntityTypeID
	 , EntityID
	 , ProcedureID
	 , SettingID
	 , SettingValueNew
	 , SettingValueOld
FROM #BuildDetailInsert;

DROP TABLE IF EXISTS #BuildDetailInsert;
SET NOCOUNT OFF;

END

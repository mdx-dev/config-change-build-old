use CAV22
go

CREATE OR ALTER PROCEDURE tix.ccb_ssp_DeployBuild (
    @BuildID int
  , @IsRollback bit
) AS

BEGIN
SET NOCOUNT ON;

DECLARE @DetailInitial tix.ccb_ut_BuildDetail
      , @DetailCurrent tix.ccb_ut_BuildDetail
      , @Ticket varchar(30) = (select top 1 Ticket from tix.ccb_Build where BuildID = @BuildID)

-- IF this is a rollback, SettingValueNew and SettingValueOld are switched
-- ELSE continue with the deploy as normal
IF @IsRollback = 1
    BEGIN
    INSERT INTO @DetailInitial (EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld)
    SELECT EntityTypeID, EntityID, ProcedureID, SettingID
	     , SettingValueOld as SettingValueNew
	     , SettingValueNew as SettingValueOld
    FROM tix.ccb_BuildDetail
    WHERE BuildID = @BuildID
	END
ELSE
    BEGIN
    INSERT INTO @DetailInitial (EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld)
    SELECT EntityTypeID, EntityID, ProcedureID, SettingID
	  , SettingValueNew
	  , SettingValueOld
    FROM tix.ccb_BuildDetail
    WHERE BuildID = @BuildID
	END

-- Grabs the current settings for all of the entities.
INSERT INTO @DetailCurrent (EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld)
EXEC tix.ccb_ssp_GetCurrentSetting @DetailInitial

-- Joining Client, Plan, and Treatment Code data needed to deploy the settings
-- Currently, deploys can be done to the config schema and Incentive Amounts table
SELECT det.EntityTypeID
     , det.EntityID
	 , det.ProcedureID
	 , det.SettingID
	 , stg.SettingName
	 , stg.SettingGroup
	 , stg.SettingSubGroup
	 , det.SettingValueNew
	 , det.SettingValueOld
	 , ISNULL(cur.SettingValueOld, 'NO_VALUE') as SettingValueCurrent
	 , cln.VitalsClientID
	 , tmc.Code as TreatmentCode
INTO #BuildDetail
FROM @DetailInitial det
     INNER JOIN tix.ccb_Setting stg
	         ON stg.SettingID = det.SettingID
     LEFT  JOIN @DetailCurrent cur
	         ON cur.EntityTypeID = det.EntityTypeID
			AND cur.EntityID = det.EntityID
			AND cur.ProcedureID = det.ProcedureID
			AND cur.SettingID = det.SettingID
	 LEFT  JOIN dbo.Clients cln
	         ON cln.Id = det.EntityID
			AND det.EntityTypeID in (1, 5)
	 LEFT  JOIN dbo.Plans pln
	         ON pln.Id = det.EntityID
			AND det.EntityTypeID in (2, 6)
	 LEFT  JOIN dbo.TreatmentCodes tmc
	         ON det.ProcedureID = tix.ccb_udf_TC_To_Proc(tmc.Code)
--declare @DifferentSettingValueCount int = (select count(*) from #BuildDetail where SettingValueOld <> SettingValueCurrent)
--if @DifferentSettingValueCount <> 0

---------------------------------------------------------------------------------------------------------------
------------------------------------------ CONFIGURATION CHANGES ----------------------------------------------
---------------------------------------------------------------------------------------------------------------

-- Prepping the configuration based settings to be deployed to the config schema tables via the config.proc_Upsert stored procedures
SELECT ROW_NUMBER() OVER (ORDER BY EntityTypeID DESC) as ConfigStep
     , det.EntityTypeID
     , CASE WHEN det.EntityTypeID in (1, 5) THEN VitalsClientID
	        WHEN det.EntityTypeID in (2, 6) THEN CAST(EntityID as varchar(255))
			ELSE NULL
		END as ConfigEntityID
	 , det.TreatmentCode
	 , det.SettingValueNew as SettingValue
	 , ctg.[Name] as SettingCategoryName
	 , def.SettingGroupKey
	 , def.[Name] as SettingDefinitionName
INTO #UpsertConfig
FROM #BuildDetail det
     INNER JOIN config.SettingDefinition def
	         ON def.[Name] = det.SettingName
	 INNER JOIN config.SettingCategory ctg
	         ON ctg.Id = def.SettingCategoryId
WHERE det.SettingGroup = 'configuration';

DECLARE @configStep int
      , @configStepMax int
      , @entityTypeID int
      , @configEntityID varchar(255) -- Single column containing PlanID's for plan configs, and VitalsClientID for clients configs
      , @settingDefinitionName varchar(255)
      , @settingValue varchar(255)
      , @treatmentCode varchar(10)
      , @settingGroupKey varchar(255)
      , @settingCategoryName varchar(255)
      , @configModifiedReason varchar(255)

SELECT @configModifiedReason = (SELECT TOP 1 Ticket FROM tix.ccb_Build WHERE BuildID = @BuildID) -- Modified Reason is the ticket name
     , @configStep =    (SELECT MIN(ConfigStep)    FROM #UpsertConfig)
	 , @configStepMax = (SELECT MAX(ConfigStep)    FROM #UpsertConfig);

-- This loop updates configurations via the config.Upsert procedures using the transformed configuration records from the build detail
WHILE @configStep <= @configStepMax
BEGIN
    SELECT @entityTypeID          = EntityTypeID         
		 , @configEntityID        = ConfigEntityID       
		 , @settingDefinitionName = SettingDefinitionName
		 , @settingValue          = SettingValue         
		 , @treatmentCode         = TreatmentCode        
		 , @settingGroupKey       = SettingGroupKey      
		 , @settingCategoryName   = SettingCategoryName  
	FROM #UpsertConfig
	WHERE ConfigStep = @configStep

	-- Client Setting Values
    IF @entityTypeID = 1
	    BEGIN
        EXEC config.proc_UpsertConfigClientSettingValues
             @VitalsClientId        = @configEntityID,
             @SettingDefinitionName = @settingDefinitionName,
             @SettingValue          = @settingValue,
             @SettingGroupKey       = @settingGroupKey,
             @SettingCategoryName   = @settingCategoryName,
             @ModifiedReason        = @configModifiedReason
	    END
	-- Plan Setting Values
    IF @entityTypeID = 2
	    BEGIN
        EXEC config.proc_UpsertConfigPlanSettingValues
             @planid                = @configEntityID,
             @SettingDefinitionName = @settingDefinitionName,
             @SettingValue          = @settingValue,
             @SettingGroupKey       = @settingGroupKey,
             @SettingCategoryName   = @settingCategoryName,
             @ModifiedReason        = @configModifiedReason
	    END
	-- Treatment Client Setting Values
    IF @entityTypeID = 5
	    BEGIN
        EXEC config.proc_UpsertConfigTreatmentClientSettingValues
             @VitalsClientId        = @configEntityID,
             @SettingDefinitionName = @settingDefinitionName,
             @SettingValue          = @settingValue,
             @TreatmentCode         = @treatmentCode,
             @SettingGroupKey       = @settingGroupKey,
             @SettingCategoryName   = @settingCategoryName,
             @ModifiedReason        = @configModifiedReason
	    END
	-- Treatment Plan Setting Values
    IF @entityTypeID = 6
	    BEGIN
        EXEC config.proc_UpsertConfigTreatmentPlanSettingValues
             @planid                = @configEntityID,
             @SettingDefinitionName = @settingDefinitionName,
             @SettingValue          = @settingValue,
             @TreatmentCode         = @treatmentCode,
             @SettingGroupKey       = @settingGroupKey,
             @SettingCategoryName   = @settingCategoryName,
             @ModifiedReason        = @configModifiedReason
	    END
    SET @configStep += 1
END

---------------------------------------------------------------------------------------------------------------
----------------------------------------- INCENTIVE AMOUNTS CHANGES -------------------------------------------
---------------------------------------------------------------------------------------------------------------

-- Transforming #BuildDetail data to fit into IncentiveAmounts
-- Grabbing IncentiveTierID's
-- Determining which records will be DELETED, UPDATED, or INSERTED
SELECT ica.Id as IncentiveAmountsID
     , ProcedureID
	 , ict.Id as IncentiveTierID
	 , CASE WHEN SettingValueNew = 'NO_VALUE' THEN NULL
	        ELSE CAST(SettingValueNew as decimal(18,2))
		END as Amount
	 , CASE WHEN SettingValueNew =  'NO_VALUE' and SettingValueOld <> 'NO_VALUE' then 'delete'
	        WHEN SettingValueNew <> 'NO_VALUE' and SettingValueOld =  'NO_VALUE' then 'insert'
			WHEN SettingValueNew <> 'NO_VALUE' and SettingValueOld <> 'NO_VALUE' then 'update'
			ELSE 'error'
		END as dml_method
INTO #UpsertIncentiveAmount
FROM #BuildDetail det
     LEFT  JOIN dbo.IncentiveTiers ict
	         ON ict.Plan_Id = det.EntityID
			AND ict.IsActive = 1 -- some incentive tiers are inactive, and there are duplicates
			AND ict.TierNumber = CASE WHEN SettingName = 'static_tier_1' THEN 1
	                                  WHEN SettingName = 'static_tier_2' THEN 2
			                          WHEN SettingName = 'static_tier_3' THEN 3
			                          ELSE NULL END
	 LEFT  JOIN dbo.IncentiveAmounts ica
	         ON ica.IncentiveTier_Id = ict.Id
			AND ica.IsActive = 1 --checks to see if the incentive amounts are inactive, which is unlikely
WHERE SettingGroup = 'incentive_amount'
  AND EntityTypeID = 6
  AND SettingValueNew <> SettingValueOld;

-- UPDATE IncentiveAmounts records
UPDATE ica
   SET ica.Amount         = upd.Amount
	 , ica.DateModified   = CURRENT_TIMESTAMP
	 , ica.ModifiedBy     = SUSER_NAME()
	 , ica.ModifiedReason = tix.ccb_udf_UpdateModifiedReason(ica.ModifiedReason, @Ticket)
FROM dbo.IncentiveAmounts ica
     INNER JOIN #UpsertIncentiveAmount upd
	         ON ica.Id = upd.IncentiveAmountsID
WHERE upd.dml_method = 'update';

-- DELETE IncentiveAmounts records
DELETE FROM dbo.IncentiveAmounts
WHERE Id IN (SELECT IncentiveAmountsID FROM #UpsertIncentiveAmount WHERE dml_method = 'delete');

-- INSERT IncentiveAmounts records
INSERT INTO dbo.IncentiveAmounts (Amount, NominalAmount, Active, IncentiveTier_Id, Procedure_Id, AddedBy, ModifiedReason, DateAdded, DateActive)
SELECT Amount, 0.00 as NominalAmount, 1 as Active, IncentiveTierID, ProcedureID, SUSER_NAME() as AddedBy
     , tix.ccb_udf_UpdateModifiedReason(NULL, @Ticket) as ModifiedReason, CURRENT_TIMESTAMP as DateAdded, CURRENT_TIMESTAMP as DateActive
FROM #UpsertIncentiveAmount
WHERE dml_method = 'insert';

-- Updates the BuildID on at the Build table
-- Sets the IsDeployed flag to 1  on deploy, 0 on rollback
-- Sets the DateLastDeployed on deploy, and DateLastRolledback on rollback
IF @IsRollback = 1
    BEGIN
    UPDATE tix.ccb_Build
	   SET IsDeployed = 0
	     , DateLastRolledBack = CURRENT_TIMESTAMP
	WHERE BuildID = @BuildID
	END
ELSE
    BEGIN
    UPDATE tix.ccb_Build
	   SET IsDeployed = 1
	     , DateLastDeployed = CURRENT_TIMESTAMP
	WHERE BuildID = @BuildID
	END;

DROP TABLE IF EXISTS #BuildDetail, #UpsertConfig, #UpsertIncentiveAmount;
SET NOCOUNT OFF;

END;
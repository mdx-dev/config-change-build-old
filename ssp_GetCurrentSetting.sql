
CREATE OR ALTER PROCEDURE tix.ccb_ssp_GetCurrentSetting (
    @Detail_GetCurrentSetting tix.ccb_ut_BuildDetail READONLY
) AS

-- ==========================================================================================
-- Description: Recieves Build Detail records and returns the current settings for them.
--              Is used in procedures that require checking current settings.
-- Parameters:
--    @tix.ccb_ut_BuildDetail
--        EntityTypeID, EntityID, ProcedureID, SettingValueNew, NULL as SettingValueOLD
-- Returns:
--    @tix.ccb_ut_BuildDetail
--        EntityTypeID, EntityID, ProcedureID, SettingValueNew, SettingValueOLD
-- ==========================================================================================

BEGIN
SET NOCOUNT ON;

-- Joining SettingDefinitionID from config.SettingDefinition using the Setting Names
SELECT dat.EntityTypeID
     , dat.EntityID
	 , dat.ProcedureID
	 , dat.SettingID
	 , dat.SettingValueNew
	 , def.Id as SettingDefinitionID
	 , def.DefaultValue
	 , stg.SettingGroup
INTO #BuildDetailConfig
FROM @Detail_GetCurrentSetting dat
     INNER JOIN tix.ccb_Setting stg
	         ON stg.SettingID = dat.SettingID
	 INNER JOIN config.SettingDefinition def
	         ON def.[Name] = stg.SettingName;

---------------------------------------------------------------------------------------------------------------
------------------------------------------ CONFIGURATION RECORDS ----------------------------------------------
---------------------------------------------------------------------------------------------------------------
-- Getting all configuration related settings for each entity type level
-- Currently only Client, Plan, Treatment Client, and Treatment Plan levels are implemented

/*********************************************** IMPORTANT NOTE ***********************************************
** Currently, since there is no way to remove configurations, SettingValueOld cannot be given a NULL or NO_VALUE
** since this would cause problems on a rollback. If there are no SettingValues for a setting at a specific
** config level (client, plan, treatment client, or treatment plan) then the SettingValue for the next highest
** priority level will be taken, if it exists.
**
** PRIORITY ORDER FROM LEFT TO RIGHT: Treatmement Plan > Treatment Client > Plan > Client > Default 
**
** EXAMPLE: A change is to be made for show_incentives = false at the Treatment Plan level
**          No setting records exist in config.TreatmentPlanSettingValue or config.TreatmentClientSettingValue
**          Records for show_incentives = true exist in config.PlanSettingValue and config.ClientSettingValue
**          SettingValueOld would be set to the value from config.PlanSettingValue.
**************************************************************************************************************/

SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld
INTO #DetailConfiguration
FROM (
    -- Client Level Configs
    SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew
	     , COALESCE(csv.SettingValue, dat.DefaultValue) as SettingValueOld
    FROM #BuildDetailConfig dat
         LEFT  JOIN config.ClientSettingValue csv
    	         ON dat.EntityID = csv.ClientId
    			AND dat.SettingDefinitionID = csv.SettingDefinitionId
    WHERE dat.EntityTypeID = 1
	  AND dat.SettingGroup = 'configuration'
    UNION
    -- Plan Level Configs
    SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew
	     , COALESCE(psv.SettingValue, csv.SettingValue, dat.DefaultValue) as SettingValueOld
    FROM #BuildDetailConfig dat
	     LEFT  JOIN dbo.Plans pln
		         ON pln.Id = dat.EntityID
	     LEFT  JOIN config.ClientSettingValue csv
		         ON pln.Client_Id = csv.ClientId
				AND dat.SettingDefinitionID = csv.SettingDefinitionId
         LEFT  JOIN config.PlanSettingValue psv
    	         ON dat.EntityID = psv.PlanId
    			AND dat.SettingDefinitionID = psv.SettingDefinitionId
    WHERE dat.EntityTypeID = 2
      AND dat.SettingGroup = 'configuration'
    UNION
    -- Treatment Client Level Configs
    SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew
	     , COALESCE(tcv.SettingValue, csv.SettingValue, dat.DefaultValue) as SettingValueOld
    FROM #BuildDetailConfig dat
	     LEFT  JOIN config.ClientSettingValue csv
		         ON dat.EntityID = csv.ClientId
				AND dat.SettingDefinitionID = csv.SettingDefinitionId
         LEFT  JOIN config.TreatmentClientSettingValue tcv
    	         ON dat.EntityID = tcv.ClientId
    			AND dat.ProcedureID = tix.ccb_udf_TC_To_Proc(tcv.TreatmentCode)
    			AND dat.SettingDefinitionID = tcv.SettingDefinitionId
    WHERE dat.EntityTypeID = 5
      AND dat.SettingGroup = 'configuration'
    UNION
    -- Treatment Plan Level Configs
    SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew
	     , COALESCE(tpv.SettingValue, tcv.SettingValue, psv.SettingValue, csv.SettingValue, dat.DefaultValue) as SettingValueOld
    FROM #BuildDetailConfig dat
	     LEFT  JOIN dbo.Plans pln
		         ON pln.Id = dat.EntityID
	     LEFT  JOIN config.ClientSettingValue csv
		         ON pln.Client_Id = csv.ClientId
				AND dat.SettingDefinitionID = csv.SettingDefinitionId
         LEFT  JOIN config.PlanSettingValue psv
    	         ON dat.EntityID = psv.PlanId
    			AND dat.SettingDefinitionID = psv.SettingDefinitionId
		 LEFT  JOIN config.TreatmentClientSettingValue tcv
		         ON dat.EntityID = tcv.ClientId
				AND dat.ProcedureID = tix.ccb_udf_TC_To_Proc(tcv.TreatmentCode)
				AND dat.SettingDefinitionID = tcv.SettingDefinitionId
         LEFT  JOIN config.TreatmentPlanSettingValue tpv
    	         ON dat.EntityID = tpv.PlanId
    			AND dat.ProcedureID = tix.ccb_udf_TC_To_Proc(tpv.TreatmentCode)
    			AND dat.SettingDefinitionID = tpv.SettingDefinitionId
    WHERE dat.EntityTypeID = 6
	  AND dat.SettingGroup = 'configuration'
) tbl;

---------------------------------------------------------------------------------------------------------------
----------------------------------------- INCENTIVE AMOUNTS RECORDS -------------------------------------------
---------------------------------------------------------------------------------------------------------------

-- Getting the current static incentive settings from IncentiveAmounts
-- This only applies to Treatment Plan level records
-- IMPORTANT NOTE: Will only check for ACTIVE records in IncentiveAmounts and IncentiveTier
SELECT EntityTypeID, EntityID, ProcedureID, dat.SettingID, SettingValueNew, ISNULL(CAST(ica.Amount as varchar(255)), 'NO_VALUE') as SettingValueOld
INTO #DetailIncentiveAmount
FROM @Detail_GetCurrentSetting dat
     INNER JOIN tix.ccb_Setting stg
	         ON stg.SettingID = dat.SettingID
	 INNER JOIN dbo.IncentiveTiers ict
	         ON ict.Plan_Id = dat.EntityID
			AND ict.TierNumber = CASE WHEN stg.SettingName LIKE 'static_tier_%' THEN RIGHT(SettingName, 1) ELSE NULL END
			AND ict.IsActive = 1
	 LEFT  JOIN dbo.IncentiveAmounts ica
	         ON ica.IncentiveTier_Id = ict.Id
			AND ica.Procedure_Id = dat.ProcedureID
			AND ica.IsActive = 1
WHERE stg.SettingGroup = 'incentive_amount'
  AND dat.EntityTypeID = 6;

---------------------------------------------------------------------------------------------------------------
---------------------------------------------- END OF PROCEDURE -----------------------------------------------
---------------------------------------------------------------------------------------------------------------
-- Combining all setting groups
SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld
INTO #Output
FROM (SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld
      FROM #DetailConfiguration
	  WHERE SettingValueNew <> SettingValueOld
      UNION
      SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld
	  FROM #DetailIncentiveAmount
	  WHERE SettingValueNew <> SettingValueOld
) tbl;

---------------------- OUTPUT -----------------------------
SELECT EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld
FROM #Output;

DROP TABLE IF EXISTS #Output, #BuildDetailConfig, #DetailConfiguration, #DetailIncentiveAmount;
SET NOCOUNT OFF;

END

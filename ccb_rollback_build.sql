use DataStage
go

create or alter procedure ccb.rollback_build (@build_id int) as
begin

set nocount on

begin tran

declare @commit_deploy bit = 1
-- Grabbing ALL records for the chosen ConfigChangeBuildID from ccb.ConfigChangeBuildDetail
drop table if exists #Build_Detail
select bls.EntityTypeID
     , ent.EntityTypeName
	 , bls.EntityID
	 , bls.EntityName
	 , bls.ProcedureID
	 , tmc.Code as TreatmentCode
	 , bls.ConfigID
	 , cfg.ConfigName
	 , cfg.ConfigGroup
	 , bls.ConfigValueNew as ConfigValueOld
	 , bls.ConfigValueOld as ConfigValueNew
	 , case when cop.ChangeOperationName = 'Insert' then (select ChangeOperationID from ccb.ChangeOperation where ChangeOperationName = 'Delete')
	        when cop.ChangeOperationName = 'Delete' then (select ChangeOperationID from ccb.ChangeOperation where ChangeOperationName = 'Insert')
			else bls.ChangeOperationID
		end as ChangeOperationID
	 , case when cop.ChangeOperationName = 'Insert' then 'Delete'
	        when cop.ChangeOperationName = 'Delete' then 'Insert'
			else cop.ChangeOperationName
		end as ChangeOperationName
	 , ccb.Ticket
into #Build_Detail
from ccb.ConfigChangeBuildDetail bls
     inner join ccb.ConfigChangeBuild ccb
	         on ccb.ConfigChangeBuildID = bls.ConfigChangeBuildID
     inner join ccb.Config cfg
	         on cfg.ConfigID = bls.ConfigID
	 inner join ccb.ChangeOperation cop
	         on cop.ChangeOperationID = bls.ChangeOperationID
	 inner join ccb.EntityType ent
	         on ent.EntityTypeID = bls.EntityTypeID
	 left  join CAV22.dbo.TreatmentCodes tmc
	         on bls.ProcedureID = (case when len(tmc.Code) = 5 then concat(9, tmc.Code) else tmc.Code end)
where bls.ConfigChangeBuildID = @build_id


---------------------------------------------------------------------------------------------------------------
------------------------------------------ CONFIGURATION CHANGES ----------------------------------------------
---------------------------------------------------------------------------------------------------------------

-- Grabbing build detail records pertaining to configurations and adding setting definitions necessary for config.Upsert procedures
drop table if exists #Config_Upserts
select EntityTypeName
     , EntityID
	 , case when EntityTypeName in ('client', 'treatment_client') then cln.VitalsClientId else null end as VitalsClientId
     , case when EntityTypeName in ('plan', 'treatment_plan') then EntityID else null end as PlanId
	 , bls.TreatmentCode
	 , def.[Name] as SettingDefinitionName
	 , def.SettingGroupKey
	 , ctg.[Name] as SettingCategoryName
	 , ConfigValueNew as SettingValue
	 , ConfigValueOld as SettingValue_Old_Build
	 , cast(coalesce(tpv.SettingValue, tcv.SettingValue, psv.SettingValue, csv.SettingValue) as varchar) as SettingValue_Old_Actual
	 , Ticket as ModifiedReason
	 , ROW_NUMBER() over (order by EntityTypeName, EntityID, TreatmentCode, SettingGroupKey, def.[Name]) as config_step
	 , 0 as config_step_complete
into #Config_Upserts
from #Build_Detail bls
     inner join CAV22.config.SettingDefinition def
	         on def.[Name] = bls.ConfigName
	 inner join CAV22.config.SettingCategory ctg
	         on ctg.Id = def.SettingCategoryId
	 left  join CAV22.dbo.Clients cln
	         on bls.EntityID = cln.Id
			and bls.EntityTypeName in ('client', 'treatment_client')
	 left  join CAV22.config.ClientSettingValue csv
	         on csv.ClientId = bls.EntityID
			and bls.EntityTypeName = 'client'
			and csv.SettingDefinitionId = def.Id
	 left  join CAV22.config.PlanSettingValue psv
	         on psv.PlanId = bls.EntityID
			and bls.EntityTypeName = 'plan'
			and psv.SettingDefinitionId = def.Id
	 left  join CAV22.config.TreatmentClientSettingValue tcv
	         on tcv.ClientId = bls.EntityID
			and bls.EntityTypeName = 'treatment_client'
			and tcv.SettingDefinitionId = def.Id
			and bls.ProcedureID = (case when len(tcv.TreatmentCode) = 5 then concat(9, tcv.TreatmentCode) else tcv.TreatmentCode end)
	 left  join CAV22.config.TreatmentPlanSettingValue tpv
	         on tpv.PlanId = bls.EntityID
			and bls.EntityTypeName = 'treatment_plan'
			and tpv.SettingDefinitionId = def.Id
			and bls.ProcedureID = (case when len(tpv.TreatmentCode) = 5 then concat(9, tpv.TreatmentCode) else tpv.TreatmentCode end)
where ConfigGroup = 'configuration'

-- if the current actual Setting Values do not match the old values in build detail, rollback
if (select count(*) from #Config_Upserts where SettingValue_Old_Build <> SettingValue_Old_Actual) > 0
    set @commit_deploy = 0
    goto End_Procedure

declare @config_step int
declare @config_step_max int
declare @entity_type_name varchar(255)
declare @vitals_client_id varchar(255)
declare @plan_id bigint
declare @setting_definition_name varchar(255)
declare @setting_value varchar(255)
declare @treatment_code varchar(10)
declare @setting_group_key varchar(255)
declare @setting_category_name varchar(255)
declare @modified_reason varchar(255)

set @config_step = (select min(config_step) from #Config_Upserts)
set @config_step_max = (select max(config_step) from #Config_Upserts)

-- This loop updates configurations via the config.Upsert procedures using the transformed configuration records from the build detail
while @config_step <= @config_step_max

begin
    set @entity_type_name =        (select EntityTypeName        from #Config_Upserts where config_step = @config_step)
    set @vitals_client_id =        (select VitalsClientID        from #Config_Upserts where config_step = @config_step)
    set @plan_id =                 (select PlanId                from #Config_Upserts where config_step = @config_step)
	set @setting_definition_name = (select SettingDefinitionName from #Config_Upserts where config_step = @config_step)
	set @setting_value =           (select SettingValue          from #Config_Upserts where config_step = @config_step)
	set @treatment_code =          (select TreatmentCode         from #Config_Upserts where config_step = @config_step)
	set @setting_group_key =       (select SettingGroupKey       from #Config_Upserts where config_step = @config_step)
	set @setting_category_name =   (select SettingCategoryName   from #Config_Upserts where config_step = @config_step)
	set @modified_reason =         (select ModifiedReason        from #Config_Upserts where config_step = @config_step)

	if @entity_type_name = 'client'           goto Config_Upsert_Client
	if @entity_type_name = 'plan'             goto Config_Upsert_Plan
	if @entity_type_name = 'treatment_client' goto Config_Upsert_TreatmentClient
	if @entity_type_name = 'treatment_plan'   goto Config_Upsert_TreatmentPlan

    Config_Upsert_Client:
        exec CAV22.config.proc_UpsertConfigClientSettingValues
             @VitalsClientId        = @vitals_client_id,
             @SettingDefinitionName = @setting_definition_name,
             @SettingValue          = @setting_value,
             @SettingGroupKey       = @setting_group_key,
             @SettingCategoryName   = @setting_category_name,
             @ModifiedReason        = @modified_reason
	    goto Config_Step_Increase

    Config_Upsert_Plan:
        exec CAV22.config.proc_UpsertConfigPlanSettingValues
             @planid                = @plan_id,
             @SettingDefinitionName = @setting_definition_name,
             @SettingValue          = @setting_value,
             @SettingGroupKey       = @setting_group_key,
             @SettingCategoryName   = @setting_category_name,
             @ModifiedReason        = @modified_reason
	    goto Config_Step_Increase

    Config_Upsert_TreatmentClient:
        exec CAV22.config.proc_UpsertConfigTreatmentClientSettingValues
             @VitalsClientId        = @vitals_client_id,
             @SettingDefinitionName = @setting_definition_name,
             @SettingValue          = @setting_value,
             @TreatmentCode         = @treatment_code,
             @SettingGroupKey       = @setting_group_key,
             @SettingCategoryName   = @setting_category_name,
             @ModifiedReason        = @modified_reason
	    goto Config_Step_Increase

    Config_Upsert_TreatmentPlan:
        exec CAV22.config.proc_UpsertConfigTreatmentPlanSettingValues
             @planid                = @plan_id,
             @SettingDefinitionName = @setting_definition_name,
             @SettingValue          = @setting_value,
             @TreatmentCode         = @treatment_code,
             @SettingGroupKey       = @setting_group_key,
             @SettingCategoryName   = @setting_category_name,
             @ModifiedReason        = @modified_reason
	    goto Config_Step_Increase

    Config_Step_Increase:
	    update #Config_Upserts set config_step_complete = 1 where config_step = @config_step
	    set @config_step += 1
end

---------------------------------------------------------------------------------------------------------------
----------------------------------------- INCENTIVE AMOUNTS CHANGES -------------------------------------------
---------------------------------------------------------------------------------------------------------------

-- Grabbing records from the build detail pertaining to Incentive Amounts and transforming them to fit into IncentiveAmounts
drop table if exists #Incentive_Amount_Upserts
select ChangeOperationID
	 , ChangeOperationName
     , EntityID as PlanID
	 , EntityName
     , ica.Id as IncentiveAmounts_Id
     , cast(ConfigValueNew as decimal(18,2)) as Amount
	 , cast(ConfigValueOld as decimal(18,2)) as Amount_Old_Current
	 , ica.Amount as Amount_Old_Actual
	 , bls.ProcedureID as Procedure_Id
	 , ict.Id as IncentiveTier_Id
	 , 0 as incentive_step_complete
	 , case when ica.ModifiedReason is not null then concat(ica.ModifiedReason, '; ', Ticket) else Ticket end as ModifiedReason
into #Incentive_Amount_Upserts
from #Build_Detail bls
     left  join CAV22.dbo.IncentiveTiers ict
	         on ict.Plan_Id = bls.EntityID
		        and ict.IsActive = 1
			and ict.TierNumber = (case when ConfigName = 'static_tier_1' then 1
			                           when ConfigName = 'static_tier_2' then 2
									   when ConfigName = 'static_tier_3' then 3
									   else null end)
	 left  join CAV22.dbo.IncentiveAmounts ica
	         on ica.Procedure_Id = bls.ProcedureID
			and ica.IncentiveTier_Id = ict.Id
where ConfigGroup = 'incentive_amounts'
  and EntityTypeName = 'treatment_plan'

-- if the current actual Incentive Amounts do not match the old values in build detail, rollback
if (select count(*) from #Incentive_Amount_Upserts where Amount_Old_Actual <> Amount_Old_Current) > 0
    set @commit_deploy = 0
    goto End_Procedure

-- Deleting IncentiveAmount records 
delete from CAV22.dbo.IncentiveAmounts
where Id in (
select IncentiveAmounts_Id
from #Incentive_Amount_Upserts
where ChangeOperationName = 'delete')

-- Adding IncentiveAmount records
insert into CAV22.dbo.IncentiveAmounts (Amount, NominalAmount, Active, IncentiveTier_Id, Procedure_Id, AddedBy, ModifiedReason, DateAdded)
select Amount, 0.00 as NominalAmount, 1 as Active, IncentiveTier_Id, Procedure_Id, user as AddedBy, ModifiedReason, getdate() as DateAdded
from #Incentive_Amount_Upserts
where ChangeOperationName = 'insert'

-- Updating IncentiveAmount records
update ica
set ica.Amount = upd.Amount
  , ica.DateModified = getdate()
  , ica.ModifiedBy = USER
  , ica.ModifiedReason = upd.ModifiedReason
from CAV22.dbo.IncentiveAmounts ica
     inner join #Incentive_Amount_Upserts upd
	         on upd.IncentiveAmounts_Id = ica.Id
where upd.ChangeOperationName = 'update'

update ccb.ConfigChangeBuild
set IsDeployed = 0, DateLastRolledBack = getdate()
where ConfigChangeBuildID = @build_id

begin
    End_Procedure:
    if @commit_deploy = 0
	    rollback
	else
	    commit
end

drop table if exists #Build_Detail
drop table if exists #Config_Upserts
drop table if exists #Incentive_Amount_Upserts

end

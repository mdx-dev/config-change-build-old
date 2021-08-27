use DataStage
go

create or alter procedure ccb.deploy_build (@build_id int) as
begin

set nocount on
/*
select *
into DataStage.ccb.PlanSettingValue_BACKUP
from CAV22.config.PlanSettingValue
where PlanId = 18722

select *
into DataStage.ccb.TreatmentPlanSettingValue_BACKUP
from CAV22.config.TreatmentPlanSettingValue
where PlanId = 18722

select a.*
into DataStage.ccb.IncentiveAmounts_BACKUP
from CAV22.dbo.IncentiveAmounts a
join CAV22.dbo.IncentiveTiers b
on a.IncentiveTier_Id = b.Id
where b.Plan_Id = 18722
*/

drop table if exists #Build_Summary
select bls.EntityTypeID
     , ent.EntityTypeName
	 , bls.EntityID
	 , bls.EntityName
	 , bls.ProcedureID
	 , tmc.Code as TreatmentCode
	 , bls.ConfigID
	 , cfg.ConfigName
	 , cfg.ConfigGroup
	 , bls.ConfigValueNew
	 , bls.ConfigValueOld
	 , bls.ChangeOperationID
	 , cop.ChangeOperationName
	 , bls.Ticket
into #Build_Summary
from ccb.ConfigChangeBuildSummary bls
     inner join ccb.Config cfg
	         on cfg.ConfigID = bls.ConfigID
	 inner join ccb.ChangeOperation cop
	         on cop.ChangeOperationID = bls.ChangeOperationID
	 inner join ccb.EntityType ent
	         on ent.EntityTypeID = bls.EntityTypeID
	 left  join CAV22.dbo.TreatmentCodes tmc
	         on bls.ProcedureID = (case when len(tmc.Code) = 5 then concat(9, tmc.Code) else tmc.Code end)
where ConfigChangeBuildID = 1

drop table if exists #Config_Upserts
select EntityTypeName
     , EntityID
	 , case when EntityTypeName in ('client', 'treatment_client') then cln.VitalsClientId else null end as VitalsClientId
     , case when EntityTypeName in ('plan', 'treatment_plan') then EntityID else null end as PlanId
	 , TreatmentCode
	 , def.[Name] as SettingDefinitionName
	 , def.SettingGroupKey
	 , ctg.[Name] as SettingCategoryName
	 , ConfigValueNew as SettingValue
	 , ConfigValueOld as SettingValue_OLD
	 , Ticket as ModifiedReason
	 , ROW_NUMBER() over (order by EntityTypeName, EntityID, TreatmentCode, SettingGroupKey, def.[Name]) as config_step
	 , 0 as config_step_complete
into #Config_Upserts
from #Build_Summary bls
     inner join CAV22.config.SettingDefinition def
	         on def.[Name] = bls.ConfigName
	 inner join CAV22.config.SettingCategory ctg
	         on ctg.Id = def.SettingCategoryId
	 left  join CAV22.dbo.Clients cln
	         on bls.EntityID = cln.Id
			and bls.EntityTypeName in ('client', 'treatment_client')
where ConfigGroup = 'configuration'

--select * from DataStage.ccb.Config_Upsert_Test_BACKUP

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

--delete from CAV22.dbo.IncentiveAmounts where IncentiveTier_Id in (select Id from CAV22.dbo.IncentiveTiers where Plan_Id = 18722)
--insert into CAV22.dbo.IncentiveAmounts (Amount, NominalAmount, Active, IncentiveTier_Id, Procedure_Id, ModifiedReason)
--select Amount, NominalAmount, Active, IncentiveTier_Id, Procedure_Id, ModifiedReason
--from DataStage.ccb.IncentiveAmounts_BACKUP

drop table if exists #Incentive_Amount_Upserts
select ChangeOperationID
	 , ChangeOperationName
     , EntityID as PlanID
	 , EntityName
     , ica.Id as IncentiveAmounts_Id
     , cast(ConfigValueNew as decimal(18,2)) as Amount
	 , bls.ProcedureID as Procedure_Id
	 , ict.Id as IncentiveTier_Id
	 , 0 as incentive_step_complete
	 , case when ica.ModifiedReason is not null then concat(ica.ModifiedReason, '; ', Ticket) else Ticket end as ModifiedReason
into #Incentive_Amount_Upserts
from #Build_Summary bls
     left  join CAV22.dbo.IncentiveTiers ict
	         on ict.Plan_Id = bls.EntityID
			and ict.TierNumber = (case when ConfigName = 'static_tier_1' then 1
			                           when ConfigName = 'static_tier_2' then 2
									   when ConfigName = 'static_tier_3' then 3
									   else null end)
	 left  join CAV22.dbo.IncentiveAmounts ica
	         on ica.Procedure_Id = bls.ProcedureID
			and ica.IncentiveTier_Id = ict.Id
where ConfigGroup = 'incentive_amounts'
  and EntityTypeName = 'treatment_plan'

delete from CAV22.dbo.IncentiveAmounts
where Id in (
select IncentiveAmounts_Id
from #Incentive_Amount_Upserts
where ChangeOperationName = 'delete')

insert into CAV22.dbo.IncentiveAmounts (Amount, NominalAmount, Active, IncentiveTier_Id, Procedure_Id, AddedBy, ModifiedReason, DateAdded)
select Amount, 0.00 as NominalAmount, 1 as Active, IncentiveTier_Id, Procedure_Id, user as AddedBy, ModifiedReason, getdate() as DateAdded
from #Incentive_Amount_Upserts
where ChangeOperationName = 'insert'

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
set IsDeployed = 1, DateLastDeployed = getdate()
where ConfigChangeBuildID = @build_id

drop table if exists #Build_Summary
drop table if exists #Config_Upserts
drop table if exists #Incentive_Amount_Upserts

end
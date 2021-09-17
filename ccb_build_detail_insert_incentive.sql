use DataStage
go

create or alter procedure ccb.build_detail_insert (@import_table_name nvarchar(255), @ticket nvarchar(255), @build_name nvarchar(255) = null) as
begin

set nocount on

drop table if exists #Incentives_Import_Stage_tmp
create table #Incentives_Import_Stage_tmp (
        [EntityTypeDisplayName]        nvarchar(255)
       ,[ClientName]                   nvarchar(255)
       ,[PlanName]                     nvarchar(255)
       ,[TreatmentCode]	               nvarchar(255)
       ,[percentage_of_savings]		   nvarchar(255)
       ,[maximum_incentive_amount]	   nvarchar(255)
       ,[minimum_incentive_amount]	   nvarchar(255)
       ,[static_tier_1]		           nvarchar(255)
       ,[static_tier_2]		           nvarchar(255)
       ,[static_tier_3]		           nvarchar(255)
       ,[IncentiveType]	               nvarchar(255)
       ,[ChangeOperationName]		   nvarchar(255)
)

-- Grab and transform the import records  into a format more suitable for SQL scripts
exec ('insert into #Incentives_Import_Stage_tmp
( EntityTypeDisplayName
, ClientName
, PlanName
, TreatmentCode
, percentage_of_savings
, maximum_incentive_amount
, minimum_incentive_amount
, static_tier_1
, static_tier_2
, static_tier_3
, IncentiveType
, ChangeOperationName)
select [Hierarchy], [Client Name], [Plan Name], [Treatment Code], [DI Savings]
     , [DI Max Incentive], [DI Min Incentive], [Static Tier 1], [Static Tier 2], [Static Tier 3]
     , [Incentive Type], [Modify Type]
from ' + @import_table_name)

-- More transformations, primarily checking for proper data types
-- Adding setting values for provider_type and show_incentives based on the IncentiveType values
drop table if exists #tmp1
select EntityTypeID, EntityTypeName, cln.Id as ClientID, ClientName, pln.Id as PlanID, PlanName
     , case when len(TreatmentCode) = 5 then concat(9, TreatmentCode) else TreatmentCode end as ProcedureID
	 , isnull(cast(percentage_of_savings as varchar), '0') as percentage_of_savings
	 , isnull(cast(maximum_incentive_amount as varchar), '0') as maximum_incentive_amount
	 , isnull(cast(minimum_incentive_amount as varchar), '0') as minimum_incentive_amount
     , isnull(cast(cast(static_tier_1 as decimal(18,2)) as varchar), 'NULL') as static_tier_1
	 , isnull(cast(cast(static_tier_2 as decimal(18,2)) as varchar), 'NULL') as static_tier_2
	 , isnull(cast(cast(static_tier_3 as decimal(18,2)) as varchar), 'NULL') as static_tier_3
     , case when IncentiveType = 'dynamic' then 'dynamic' else 'smartshopper' end as provider_type
	 , case when IncentiveType = 'cto' then 'false' else 'true' end as show_incentives
	 , ChangeOperationID
into #tmp1
from #Incentives_Import_Stage_tmp dat
     inner join CAV22.dbo.Clients cln
	         on cln.[Name] = dat.ClientName
	 inner join CAV22.dbo.Plans pln
	         on pln.[Name] = dat.PlanName
			and cln.Id = pln.Client_Id
     inner join DataStage.ccb.EntityType ent
	         on ent.EntityTypeDisplayName = dat.EntityTypeDisplayName
	 inner join DataStage.ccb.ChangeOperation cop
	         on cop.ChangeOperationName = dat.ChangeOperationName

-- Unpivoting the import records
drop table if exists #Unpivot_New
select EntityTypeID, EntityTypeName, EntityID, EntityName, ProcedureID, ChangeOperationID, ConfigName
     , case when ConfigValue = 'NULL' then NULL else ConfigValue end as ConfigValue
into #Unpivot_New
from (
select EntityTypeID, EntityTypeName, ClientID as EntityID, ClientName as EntityName
     , ProcedureID
	 , cast(percentage_of_savings    as varchar) as percentage_of_savings
	 , cast(maximum_incentive_amount as varchar) as maximum_incentive_amount
	 , cast(minimum_incentive_amount as varchar) as minimum_incentive_amount
	 , cast(provider_type as varchar) as provider_type
	 , cast(show_incentives as varchar) as show_incentives
	 , cast(static_tier_1 as varchar) as static_tier_1
	 , cast(static_tier_2 as varchar) as static_tier_2
	 , cast(static_tier_3 as varchar) as static_tier_3
	 , ChangeOperationID
from #tmp1
where EntityTypeName in ('client', 'treatment_client')
union
select EntityTypeID, EntityTypeName, PlanID as EntityID, PlanName as EntityName
     , ProcedureID
	 , cast(percentage_of_savings    as varchar) as percentage_of_savings
	 , cast(maximum_incentive_amount as varchar) as maximum_incentive_amount
	 , cast(minimum_incentive_amount as varchar) as minimum_incentive_amount
	 , cast(provider_type as varchar) as provider_type
	 , cast(show_incentives as varchar) as show_incentives
	 , cast(static_tier_1 as varchar) as static_tier_1
	 , cast(static_tier_2 as varchar) as static_tier_2
	 , cast(static_tier_3 as varchar) as static_tier_3
	 , ChangeOperationID
from #tmp1
where EntityTypeName in ('plan', 'treatment_plan')
) t
unpivot
(ConfigValue for ConfigName in (show_incentives, provider_type, percentage_of_savings, maximum_incentive_amount, minimum_incentive_amount
                              , static_tier_1, static_tier_2, static_tier_3)
) u

-- Transforming the imported configuration records so they will fit into ccb.ConfigChangeBuildDetail, as well as grabbing the previous setting values
drop table if exists #Configurations
select EntityTypeID
     , EntityTypeName
	 , EntityID
	 , EntityName
	 , ProcedureID
	 , cfg.ConfigID
	 , tbl.ConfigName
	 , cast(ConfigValue as varchar) as ConfigValueNew
	 , cast(coalesce(tpv.SettingValue, tcv.SettingValue, psv.SettingValue, csv.SettingValue) as varchar) as ConfigValueOld
	 , tbl.ChangeOperationID
into #Configurations
from #Unpivot_New tbl
     inner join ccb.Config cfg
	         on cfg.ConfigName = tbl.ConfigName
	 inner join CAV22.config.SettingDefinition def
	         on def.[Name] = cfg.ConfigName
	 left  join CAV22.config.ClientSettingValue csv
	         on csv.ClientId = tbl.EntityID
			and tbl.EntityTypeName = 'client'
			and csv.SettingDefinitionId = def.Id
	 left  join CAV22.config.PlanSettingValue psv
	         on psv.PlanId = tbl.EntityID
			and tbl.EntityTypeName = 'plan'
			and psv.SettingDefinitionId = def.Id
	 left  join CAV22.config.TreatmentClientSettingValue tcv
	         on tcv.ClientId = tbl.EntityID
			and tbl.EntityTypeName = 'treatment_client'
			and tcv.SettingDefinitionId = def.Id
			and tbl.ProcedureID = (case when len(tcv.TreatmentCode) = 5 then concat(9, tcv.TreatmentCode) else tcv.TreatmentCode end)
	 left  join CAV22.config.TreatmentPlanSettingValue tpv
	         on tpv.PlanId = tbl.EntityID
			and tbl.EntityTypeName = 'treatment_plan'
			and tpv.SettingDefinitionId = def.Id
			and tbl.ProcedureID = (case when len(tpv.TreatmentCode) = 5 then concat(9, tpv.TreatmentCode) else tpv.TreatmentCode end)
where cfg.ConfigGroup = 'configuration'

-- Transforming the imported incentive amount values so they will fit into ccb.ConfigChangeBuildDetail, as well as grabbing the previous setting values
drop table if exists #Incentive_Amounts
select EntityTypeID
     , EntityTypeName
	 , EntityID
	 , EntityName
	 , ProcedureID
	 , cfg.ConfigID
	 , tbl.ConfigName
	 , cast(ConfigValue as varchar) as ConfigValueNew
	 , cast(ica.Amount as varchar) as ConfigValueOld
	 , tbl.ChangeOperationID
into #Incentive_Amounts
from #Unpivot_New tbl
     inner join ccb.Config cfg
	         on cfg.ConfigName = tbl.ConfigName
	 left join CAV22.dbo.IncentiveTiers ict
	         on ict.Plan_Id = EntityID
			and ict.IsActive = 1
	        and ict.TierNumber = (case when tbl.ConfigName = 'static_tier_1' then 1
			                           when tbl.ConfigName = 'static_tier_2' then 2
									   when tbl.ConfigName = 'static_tier_3' then 3
									   else null end)
	 left join CAV22.dbo.IncentiveAmounts ica
	         on ica.Procedure_Id = tbl.ProcedureID
			and ica.IncentiveTier_Id = ict.Id
where cfg.ConfigGroup = 'incentive_amounts'
  and tbl.EntityTypeName = 'treatment_plan'

-- Combining all transformed record types into a single table to insert into ccb.ConfigChangeBuildDetail
drop table if exists #Detail_Insert
select EntityTypeID
     , EntityID
	 , EntityName
	 , ProcedureID
	 , ConfigID
	 , ConfigValueNew
	 , ConfigValueOld
	 , case when ConfigValueNew is null and ConfigValueOld is not null then (select ChangeOperationID from ccb.ChangeOperation where ChangeOperationName = 'delete')
	        when ConfigValueNew is not null and ConfigValueOld is null then (select ChangeOperationID from ccb.ChangeOperation where ChangeOperationName = 'insert')
			else (select ChangeOperationID from ccb.ChangeOperation where ChangeOperationName = 'update')
		end as ChangeOperationID
into #Detail_Insert
from (select * from #Configurations
      union all
	  select * from #Incentive_Amounts) tbl
where isnull(ConfigValueNew, 'NULL') <> isnull(ConfigValueOld, 'NULL')


-------------------------------------------------------- INSERT PROCESS --------------------------------------------------------
begin tran

-- Automatically sets the build name if none are given
if @build_name is null
set @build_name = (select concat('Build ', (isnull(max(ConfigChangeBuildID), 0) + 1)) from DataStage.ccb.ConfigChangeBuild)

-- Inserts the new build name and ID into ccb.ConfigChangeBuild and keeps track of the new ID to use for the records going into the build detail
insert into DataStage.ccb.ConfigChangeBuild (BuildName)
values (@build_name)
declare @build_id int = (select top 1 ConfigChangeBuildID
                         from DataStage.ccb.ConfigChangeBuild
						 where DateAdded = (select max(DateAdded) from DataStage.ccb.ConfigChangeBuild))

insert into DataStage.ccb.ConfigChangeBuildDetail (
	ConfigChangeBuildID,
	EntityTypeID,
	EntityID,
	EntityName,
	ProcedureID,
	ConfigID,
	ConfigValueNew,
	ConfigValueOld,
	ChangeOperationID)
select @build_id as ConfigChangeBuildID
     , EntityTypeID
     , EntityID
	 , EntityName
	 , ProcedureID
	 , ConfigID
	 , ConfigValueNew
	 , ConfigValueOld
     , ChangeOperationID
from #Detail_Insert
commit

declare @detail_insert_count nvarchar(255) = (select cast(count(*) as nvarchar) from #Detail_Insert)

drop table if exists #Incentives_Import_Stage
drop table if exists #Incentives_Import_Stage_tmp
drop table if exists #Detail_Insert
drop table if exists #tmp1
drop table if exists #Unpivot_New
drop table if exists #Unpivot_Old



declare @import_table_clean nvarchar(255) = replace(replace(@import_table_name, '[', ''), ']', '')

declare @table_split_count int = (select (len(@import_table_clean) - len(replace(@import_table_clean, '.', ''))))
declare @first_split int
declare @second_split int
declare @import_database nvarchar(255)
declare @import_schema nvarchar(255)
declare @import_table nvarchar(255)
declare @import_table_full nvarchar(255)
if @table_split_count = 2
	set @first_split = CHARINDEX('.', @import_table_clean)
	set @second_split = CHARINDEX('.', @import_table_clean, @first_split + 1)
    set @import_database =  substring(@import_table_clean, 1, @first_split - 1)
	set @import_schema = substring(@import_table_clean, @first_split + 1, @second_split - @first_split - 1)
	set @import_table = substring(@import_table_clean, @second_split + 1, len(@import_table_clean))
    set @import_table_full = '[' + @import_database + '].[' + @import_schema + '].[' + @import_table + ']'

declare @import_table_full_print_string nvarchar(max) = N'Import Table Name: ' + @import_table_full
declare @build_name_print_string nvarchar(max) = N'Build Name: ' + @build_name
declare @build_id_print_string nvarchar(max) = N'Build ID: ' + cast(@build_id as nvarchar)
declare @detail_insert_count_print nvarchar(max) = N'ConfigChangeBuildDetail Insert Count: ' + @detail_insert_count

print ( char(13) +
       N'------------------------------------------------------------------------------------------' + char(13) +
       N'---------------------------- BUILD SUMMARY INCENTIVES IMPORT -----------------------------' + char(13) +
	   N'------------------------------------------------------------------------------------------' + char(13))
print @import_table_full_print_string
print @build_id_print_string
print @build_name_print_string
print @detail_insert_count_print

end

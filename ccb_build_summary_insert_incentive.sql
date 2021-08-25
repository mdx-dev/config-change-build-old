use DataStage
go

create or alter procedure ccb.build_summary_insert (@import_table_name nvarchar(255), @build_name nvarchar(255) = null) as
begin

set nocount on
--declare @import_table_name varchar(max) = '[DataStage].dbo.[IncentiveModifier_20210819_NO_ERRORS]'

drop table if exists #Incentives_Import_Stage_tmp
create table #Incentives_Import_Stage_tmp (
        [EntityTypeName]               nvarchar(255)
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
       ,[Ticket]			           nvarchar(255)
)


exec ('insert into #Incentives_Import_Stage_tmp
( EntityTypeName
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
, ChangeOperationName
, Ticket)
select [Hierarchy], [Client Name], [Plan Name], [Treatment Code], [DI Savings]
     , [DI Max Incentive], [DI Min Incentive], [Static Tier 1], [Static Tier 2], [Static Tier 3]
     , [Incentive Type], [Modify Type], Ticket
from ' + @import_table_name)

drop table if exists #tmp1
select EntityTypeID, cln.Id as ClientID, ClientName, pln.Id as PlanID, PlanName
     , case when len(TreatmentCode) = 5 then concat(9, TreatmentCode) else TreatmentCode end as ProcedureID
	 , percentage_of_savings, maximum_incentive_amount, minimum_incentive_amount
     , cast(cast(static_tier_1 as decimal(18,2)) as nvarchar) as static_tier_1
	 , cast(cast(static_tier_2 as decimal(18,2)) as nvarchar) as static_tier_2
	 , cast(cast(static_tier_3 as decimal(18,2)) as nvarchar) as static_tier_3
	 , IncentiveType
	 , ChangeOperationID
	 , Ticket
into #tmp1
from #Incentives_Import_Stage_tmp dat
     inner join CAV22.dbo.Clients cln
	         on cln.[Name] = dat.ClientName
	 inner join CAV22.dbo.Plans pln
	         on pln.[Name] = dat.PlanName
			and cln.Id = pln.Client_Id
     inner join DataStage.ccb.EntityType ent
	         on ent.EntityTypeName = dat.EntityTypeName
	 inner join DataStage.ccb.ChangeOperation cop
	         on cop.ChangeOperationName = dat.ChangeOperationName

drop table if exists #Incentives_Import_Stage
select *
     , case when IncentiveType = 'dynamic' then 'dynamic' else 'smartshopper' end as provider_type
	 , case when IncentiveType = 'cto' then 'false' else 'true' end as show_incentives
into #Incentives_Import_Stage
from (select EntityTypeID, ClientID as EntityID, ClientName as EntityName
           , ProcedureID
      	   , isnull(percentage_of_savings, 0) as percentage_of_savings
		   , isnull(maximum_incentive_amount, 0) as maximum_incentive_amount
		   , isnull(minimum_incentive_amount, 0) as minimum_incentive_amount
      	   , static_tier_1, static_tier_2, static_tier_3, IncentiveType, ChangeOperationID, Ticket
      from #tmp1
      where EntityTypeID in (2, 4)
      union
      select EntityTypeID, PlanID as EntityID, PlanName as EntityName
           , ProcedureID
      	   , isnull(percentage_of_savings, 0) as percentage_of_savings
		   , isnull(maximum_incentive_amount, 0) as maximum_incentive_amount
		   , isnull(minimum_incentive_amount, 0) as minimum_incentive_amount
      	   , static_tier_1, static_tier_2, static_tier_3, IncentiveType, ChangeOperationID, Ticket
      from #tmp1
      where EntityTypeID in (3, 5)) t

drop table if exists #Unpivot_New
select EntityTypeID, EntityID, EntityName, ProcedureID, Ticket, ChangeOperationID, ConfigName, ConfigValue
into #Unpivot_New
from (
select EntityTypeID, EntityID, EntityName, ProcedureID, Ticket, ChangeOperationID
     , cast(show_incentives as varchar) as show_incentives
	 , cast(provider_type as varchar) as provider_type
	 , cast(percentage_of_savings as varchar) as percentage_of_savings
	 , cast(maximum_incentive_amount as varchar) as maximum_incentive_amount
	 , cast(minimum_incentive_amount as varchar) as minimum_incentive_amount
     , case when EntityTypeID = 5 then isnull(cast(static_tier_1 as varchar), 'NULL') else null end as static_tier_1
	 , case when EntityTypeID = 5 then isnull(cast(static_tier_2 as varchar), 'NULL') else null end as static_tier_2
	 , case when EntityTypeID = 5 then isnull(cast(static_tier_3 as varchar), 'NULL') else null end as static_tier_3
from #Incentives_Import_Stage
) t
unpivot
(ConfigValue for ConfigName in (show_incentives, provider_type, percentage_of_savings, maximum_incentive_amount, minimum_incentive_amount
                              , static_tier_1, static_tier_2, static_tier_3)
) u

drop table if exists #Unpivot_Old
select EntityTypeID, EntityID, ProcedureID, ConfigName, ConfigValue
into #Unpivot_Old
from (
select tbl.EntityTypeID, tbl.EntityID, tbl.ProcedureID
     , cast(show_incentives as varchar) as show_incentives
	 , cast(provider_type as varchar) as provider_type
	 , cast(percentage_of_savings as varchar) as percentage_of_savings
	 , cast(maximum_incentive_amount as varchar) as maximum_incentive_amount
	 , cast(minimum_incentive_amount as varchar) as minimum_incentive_amount
     , case when tbl.EntityTypeID = 5 then isnull(cast(static_tier_1 as varchar), 'NULL') else null end as static_tier_1
	 , case when tbl.EntityTypeID = 5 then isnull(cast(static_tier_2 as varchar), 'NULL') else null end as static_tier_2
	 , case when tbl.EntityTypeID = 5 then isnull(cast(static_tier_3 as varchar), 'NULL') else null end as static_tier_3
from (select distinct EntityTypeID, EntityID, ProcedureID from #Unpivot_New) tbl
     inner join DataStage.ccb.vw_IncentiveType_QA dat
	         on dat.EntityID = tbl.EntityID
			and dat.EntityTypeID = tbl.EntityTypeID
			and isnull(dat.ProcedureID, 0) = isnull(tbl.ProcedureID, 0)
) t
unpivot
(ConfigValue for ConfigName in (show_incentives, provider_type, percentage_of_savings, maximum_incentive_amount, minimum_incentive_amount
                              , static_tier_1, static_tier_2, static_tier_3)
) u

drop table if exists #Summary_Insert
select 
       new.EntityTypeID
     , new.EntityID
	 , new.EntityName
	 , new.ProcedureID
	 , new.ChangeOperationID
	 , con.ConfigID
	 , new.ConfigName
	 , new.ConfigValue as ConfigValueNew
	 , old.ConfigValue as ConfigValueOld
	 , new.Ticket
into #Summary_Insert
from #Unpivot_New new
     inner join DataStage.ccb.Config con
	         on con.ConfigName = new.ConfigName
     left  join #Unpivot_Old old
	         on new.EntityID = old.EntityID
			and new.EntityTypeID = old.EntityTypeID
			and isnull(new.ProcedureID, 0) = isnull(old.ProcedureID, 0)
			and new.ConfigName = old.ConfigName

begin tran
if @build_name is null
set @build_name = (select concat('Build ', (isnull(max(ConfigChangeBuildID), 0) + 1)) from DataStage.ccb.ConfigChangeBuild)


insert into DataStage.ccb.ConfigChangeBuild (BuildName)
--output inserted.ConfigChangeBuildID
values (@build_name)
declare @build_id int = (select top 1 ConfigChangeBuildID
                         from DataStage.ccb.ConfigChangeBuild
						 where DateAdded = (select max(DateAdded) from DataStage.ccb.ConfigChangeBuild))

insert into DataStage.ccb.ConfigChangeBuildSummary (
	ConfigChangeBuildID,
	EntityTypeID,
	EntityID,
	EntityName,
	ProcedureID,
	ConfigID,
	ConfigValueNew,
	ConfigValueOld,
	ChangeOperationID,
	Ticket)
select @build_id as ConfigChangeBuildID
     , EntityTypeID
	 , EntityID
	 , EntityName
	 , ProcedureID
	 , ConfigID
	 , ConfigValueNew
	 , ConfigValueOld
	 , ChangeOperationID
	 , Ticket
from #Summary_Insert
commit

declare @summary_insert_count nvarchar(255) = (select cast(count(*) as nvarchar) from #Summary_Insert)

drop table #Incentives_Import_Stage
drop table #Incentives_Import_Stage_tmp
drop table #Summary_Insert
drop table #tmp1
drop table #Unpivot_New
drop table #Unpivot_Old



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
declare @summary_insert_count_print nvarchar(max) = N'ConfigChangeBuildSummary Insert Count: ' + @summary_insert_count

print ( char(13) +
       N'------------------------------------------------------------------------------------------' + char(13) +
       N'---------------------------- BUILD SUMMARY INCENTIVES IMPORT -----------------------------' + char(13) +
	   N'------------------------------------------------------------------------------------------' + char(13))
print @import_table_full_print_string
print @build_id_print_string
print @build_name_print_string
print @summary_insert_count_print
end
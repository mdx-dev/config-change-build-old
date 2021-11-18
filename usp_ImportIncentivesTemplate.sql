use CAV22
go

create or alter procedure tix.ccb_usp_ImportIncentivesTemplate (
    @ImportTableName varchar(255)-- = 'DataStage.dbo.UAT_CCB_Test_001_Sample_Update_CORRECT_QA'
   ,@BuildName varchar(100) = ''
   ,@Ticket varchar(30) = null
   ,@Notes varchar(255) = null
) as

begin

set nocount on;

declare @DetailInitial tix.ccb_ut_BuildDetail

drop table if exists #ImportTableStage
create table #ImportTableStage (
        [EntityTypeDisplayName]        varchar(255)
       ,[ClientName]                   varchar(255)
       ,[PlanName]                     varchar(255)
       ,[TreatmentCode]	               varchar(255)
	   ,[TreatmentName]                varchar(255)
       ,[percentage_of_savings]		   varchar(255)
       ,[maximum_incentive_amount]	   varchar(255)
       ,[minimum_incentive_amount]	   varchar(255)
       ,[static_tier_1]		           varchar(255)
       ,[static_tier_2]		           varchar(255)
       ,[static_tier_3]		           varchar(255)
       ,[IncentiveType]	               varchar(255)
)

-- Grab and transform the import records  into a format more suitable for SQL scripts
exec ('
insert into #ImportTableStage
( EntityTypeDisplayName
, ClientName
, PlanName
, TreatmentCode
, TreatmentName
, percentage_of_savings
, maximum_incentive_amount
, minimum_incentive_amount
, static_tier_1
, static_tier_2
, static_tier_3
, IncentiveType)
select [Hierarchy], [Client Name], [Plan Name], [Treatment Code], [Treatment Name], [DI Savings]
     , [DI Max Incentive], [DI Min Incentive], [Static Tier 1], [Static Tier 2], [Static Tier 3], [Incentive Type]
from ' + @ImportTableName)

insert into @DetailInitial (EntityTypeID, EntityID, ProcedureID, SettingID, SettingValueNew, SettingValueOld)
select EntityTypeID
     , EntityID
	 , ProcedureID
	 --, cast(case when ProcedureID = 'NULL' then NULL else ProcedureID end as bigint) as ProcedureID
	 , SettingID
	 , SettingValueNew
     --, case when SettingValueNew = 'NULL' then NULL else SettingValueNew end as SettingValueNew
	 , null as SettingValueOld
from (
select cast(ent.EntityTypeID as varchar(255)) as EntityTypeID
     , cast(case when ent.EntityTypeID in (1, 5) then cln.Id when ent.EntityTypeID in (2, 6) then pln.Id else 'NO_SETTING_VALUE' end as varchar(255)) as EntityID
	 , cast(case when dat.TreatmentCode is null then -1
	        when len(dat.TreatmentCode) = 5 then concat(9, dat.TreatmentCode)
			else dat.TreatmentCode
		end as varchar(255)) as ProcedureID
	 , isnull(cast(percentage_of_savings as varchar(255)), '0') as percentage_of_savings
	 , isnull(cast(maximum_incentive_amount as varchar(255)), '0') as maximum_incentive_amount
	 , isnull(cast(minimum_incentive_amount as varchar(255)), '0') as minimum_incentive_amount
     , case when EntityTypeID = 6 then isnull(cast(cast(static_tier_1 as decimal(18,2)) as varchar(255)), 'NO_VALUE') else NULL end as static_tier_1
	 , case when EntityTypeID = 6 then isnull(cast(cast(static_tier_2 as decimal(18,2)) as varchar(255)), 'NO_VALUE') else NULL end as static_tier_2
	 , case when EntityTypeID = 6 then isnull(cast(cast(static_tier_3 as decimal(18,2)) as varchar(255)), 'NO_VALUE') else NULL end as static_tier_3
     , cast(case when IncentiveType = 'dynamic' then 'dynamic' else 'smartshopper' end as varchar(255)) as provider_type
	 , cast(case when IncentiveType = 'cto' then 'false' else 'true' end as varchar(255)) as show_incentives
from #ImportTableStage dat
     left  join tix.ccb_EntityType ent
	         on ent.EntityTypeDisplayName = dat.EntityTypeDisplayName
     left  join dbo.Clients cln
	         on cln.[Name] = dat.ClientName
	 left  join dbo.Plans pln
	         on pln.[Name] = dat.PlanName
			and pln.Client_Id = cln.Id
) t
unpivot
(SettingValueNew for SettingName in (show_incentives, provider_type, percentage_of_savings, maximum_incentive_amount, minimum_incentive_amount
                              , static_tier_1, static_tier_2, static_tier_3)
) u
inner join tix.ccb_Setting stg
        on stg.SettingName = u.SettingName

exec tix.ccb_ssp_CreateBuild
   @DetailInitial
 , @BuildName = @BuildName
 , @Ticket = @Ticket
 , @ImportType = 'Incentives Template'
 , @Notes = @Notes;

set nocount off;
end
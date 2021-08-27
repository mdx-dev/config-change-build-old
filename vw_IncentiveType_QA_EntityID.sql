USE [DataStage]
GO

/****** Object:  View [dbo].[vw_IncentiveType_QA]    Script Date: 8/24/2021 6:35:58 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create or alter view [ccb].[vw_IncentiveType_QA] as

select EntityTypeID
     --, ClientID
	 --, t1.PlanID
	 , t1.EntityID
	 , t1.ProcedureID
	 --, t1.TreatmentCode
	 , show_incentives
	 , provider_type
	 , minimum_incentive_amount
	 , maximum_incentive_amount
	 , percentage_of_savings
	 , static_tier_1
	 , static_tier_2
	 , static_tier_3
	 , case when provider_type = 'smartshopper'
			 and show_incentives = 'false'
			 and t2.ProcedureID is null
			then 1
	        when EntityTypeID = (select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName = 'treatment_plan')
			 and provider_type = 'smartshopper'
			 and show_incentives = 'true'
			 and t2.ProcedureID is not null
			then 2
			when EntityTypeID in ((select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName in ('treatment_client', 'plan', 'client')))
			 and provider_type = 'smartshopper'
			 and show_incentives = 'true'
			then 2
			when provider_type = 'dynamic'
			 and show_incentives = 'true'
			 and maximum_incentive_amount <> 0
			 and minimum_incentive_amount <> 0
			 and percentage_of_savings <> 0
			 and t2.ProcedureID is null
			then 3
			else 0
		end as IncentiveType
from (
select EntityTypeID
     --, ClientID
	 --, PlanID
	 , EntityID
	 --, TreatmentCode
	 , ProcedureID
	 , cast([47] as varchar(6)) as show_incentives
	 , cast([67] as varchar(12)) as provider_type
	 , cast(cast([68] as decimal(18,2)) as smallint) as minimum_incentive_amount
	 , cast(cast([69] as decimal(18,2)) as smallint) as maximum_incentive_amount
	 , cast(cast([70] as decimal(18,2)) as smallint) as percentage_of_savings
from (
select (select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName = 'treatment_plan') as EntityTypeID
     --, tbl.ClientID
     , tbl.PlanID as EntityID
	 --, tbl.TreatmentCode
	 , tbl.ProcedureID
	 , def.Id as SettingDefinitionID
	 , coalesce(tpv.SettingValue, tcv.SettingValue, psv.SettingValue, csv.SettingValue, def.DefaultValue) as SettingValue
from (
select ctc.Client_ID as ClientID
     , pln.Id as PlanID
	 --, ctc.TreatmentCode
	 , cast((case when len(ctc.TreatmentCode) = 5 then concat(9, ctc.TreatmentCode) else ctc.TreatmentCode end) as int) as ProcedureID
from CAV22.dbo.ClientTreatmentCodes ctc
     inner join CAV22.dbo.Clients cln
	         on cln.Id = ctc.Client_ID
	 inner join CAV22.dbo.Plans pln
	         on pln.Client_Id = ctc.Client_ID
where ctc.IsActive = 1
  and cln.IsActive = 1
  and pln.IsActive = 1
union
select pln.Client_Id as ClientID
     , ptc.Plan_Id as PlanID
	 --, ptc.TreatmentCode
	 , cast((case when len(ptc.TreatmentCode) = 5 then concat(9, ptc.TreatmentCode) else ptc.TreatmentCode end) as int) as ProcedureID
from CAV22.dbo.PlanTreatmentCodes ptc
     inner join CAV22.dbo.Plans pln
	         on pln.Id = ptc.Plan_Id
     inner join CAV22.dbo.Clients cln
	         on cln.Id = pln.Client_Id
where pln.IsActive = 1
  and cln.IsActive = 1
) tbl
    cross join (select Id, DefaultValue from CAV22.config.SettingDefinition where Id in (47, 67, 68, 69, 70)) def
	left  join CAV22.config.ClientSettingValue csv
	        on csv.ClientId = tbl.ClientID
		   and csv.SettingDefinitionId = def.Id
	left  join CAV22.config.PlanSettingValue psv
	        on psv.PlanId = tbl.PlanID
		   and psv.SettingDefinitionId = def.Id
	left  join CAV22.config.TreatmentClientSettingValue tcv
	        on tcv.ClientId = tbl.ClientID
		   and tcv.SettingDefinitionId = def.Id
		   and cast((case when len(tcv.TreatmentCode) = 5 then concat(9, tcv.TreatmentCode) else tcv.TreatmentCode end) as int) = tbl.ProcedureID
		   --and tcv.TreatmentCode = tbl.TreatmentCode
	left  join CAV22.config.TreatmentPlanSettingValue tpv
	        on tpv.PlanId = tbl.PlanID
		   and tpv.SettingDefinitionId = def.Id
		   and cast((case when len(tpv.TreatmentCode) = 5 then concat(9, tpv.TreatmentCode) else tpv.TreatmentCode end) as int) = tbl.ProcedureID
		   --and tpv.TreatmentCode = tbl.TreatmentCode
union all

select (select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName = 'treatment_client') as EntityTypeID
     , tbl.ClientID as EntityID
	 --, null as PlanID
	 --, tbl.TreatmentCode
	 , tbl.ProcedureID
	 , def.Id as SettingDefinitionID
	 , coalesce(tcv.SettingValue, csv.SettingValue, def.DefaultValue) as SettingValue
from (
select ctc.Client_ID as ClientID
     --, TreatmentCode
	 , cast((case when len(ctc.TreatmentCode) = 5 then concat(9, ctc.TreatmentCode) else ctc.TreatmentCode end) as int) as ProcedureID
from CAV22.dbo.ClientTreatmentCodes ctc
where ctc.IsActive = 1
  and ctc.Client_ID in (select Id from CAV22.dbo.Clients where IsActive = 1 and Id in (select distinct Client_Id from CAV22.dbo.Plans where IsActive = 1))
) tbl
    cross join (select Id, DefaultValue from CAV22.config.SettingDefinition where Id in (47, 67, 68, 69, 70)) def
	left  join CAV22.config.ClientSettingValue csv
	        on csv.ClientId = tbl.ClientID
		   and csv.SettingDefinitionId = def.Id
	left  join CAV22.config.TreatmentClientSettingValue tcv
	        on tcv.ClientId = tbl.ClientID
		   and tcv.SettingDefinitionId = def.Id
		   and cast((case when len(tcv.TreatmentCode) = 5 then concat(9, tcv.TreatmentCode) else tcv.TreatmentCode end) as int) = tbl.ProcedureID
		   --and tcv.TreatmentCode = tbl.TreatmentCode

union all

select (select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName = 'plan') as EntityTypeID
     --, pln.Client_Id as ClientID
     , pln.Id as EntityID
	 --, null as TreatmentCode
	 , null as ProcedureID
	 , def.Id as SettingDefinitionID
	 , coalesce(psv.SettingValue, csv.SettingValue, def.DefaultValue) as SettingValue
from CAV22.dbo.Plans pln
    inner join CAV22.dbo.Clients cln
	        on cln.Id = pln.Client_Id
    cross join (select Id, DefaultValue from CAV22.config.SettingDefinition where Id in (47, 67, 68, 69, 70)) def
	left  join CAV22.config.ClientSettingValue csv
	        on csv.ClientId = pln.Client_ID
		   and csv.SettingDefinitionId = def.Id
	left  join CAV22.config.PlanSettingValue psv
	        on psv.PlanId = pln.Id
		   and psv.SettingDefinitionId = def.Id
where pln.IsActive = 1
  and cln.IsActive = 1
union all

select (select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName = 'client') as EntityTypeID
     , tbl.ClientID as EntityID
	 --, null as TreatmentCode
	 , null as ProcedureID
	 , def.Id as SettingDefinitionID
	 , coalesce(csv.SettingValue, def.DefaultValue) as SettingValue
from (select Id as ClientID from CAV22.dbo.Clients where IsActive = 1 and Id in (select distinct Client_Id from CAV22.dbo.Plans where IsActive = 1)) tbl
    cross join (select Id, DefaultValue from CAV22.config.SettingDefinition where Id in (47, 67, 68, 69, 70)) def
	left  join CAV22.config.ClientSettingValue csv
	        on csv.ClientId = tbl.ClientID
		   and csv.SettingDefinitionId = def.Id
) t
pivot(
max(SettingValue) for SettingDefinitionID in ([47], [67], [68], [69], [70])
) p
) t1
left  join (
select PlanID, ProcedureID, [1] as static_tier_1, [2] as static_tier_2, [3] as static_tier_3
from (
select ict.Plan_Id as PlanID
     , ict.TierNumber
	 , icm.Procedure_Id as ProcedureID
	 , icm.Amount as IncentiveAmount
from CAV22.dbo.IncentiveAmounts icm
     inner join CAV22.dbo.IncentiveTiers ict
	         on ict.Id = icm.IncentiveTier_Id
where ict.Plan_Id in (select Id from CAV22.dbo.Plans where IsActive = 1 and Client_Id in (select Id from CAV22.dbo.Clients where IsActive = 1))
) tbl
pivot(
max(IncentiveAmount)
for TierNumber in ([1], [2], [3])
) pvt
) t2
on t1.EntityTypeID = (select EntityTypeID from DataStage.ccb.EntityType where EntityTypeName = 'treatment_plan')
and t2.PlanID = t1.EntityID
and t2.ProcedureID = t1.ProcedureID
GO

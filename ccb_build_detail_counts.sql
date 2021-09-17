use DataStage
go

create or alter procedure ccb.build_summary_counts @build_id int as
begin

set nocount on

drop table if exists #Build_Detail
select dat.EntityTypeID
     , EntityTypeName
	 , EntityID
	 , EntityName
	 , ProcedureID
	 , ptg.Code as ProcedureCategory
	 , tmc.Code as TreatmentCode
	 , tmc.[Name] as TreatmentName
	 , dat.ConfigID
	 , ConfigName
	 , ConfigGroup
	 , ConfigValueNew
	 , ConfigValueOld
	 , cop.ChangeOperationID
	 , ChangeOperationName
into #Build_Detail
from DataStage.ccb.ConfigChangeBuildDetail dat
     inner join DataStage.ccb.Config con
	         on con.ConfigID = dat.ConfigID
	 inner join DataStage.ccb.EntityType ent
	         on ent.EntityTypeID = dat.EntityTypeID
	 inner join DataStage.ccb.ChangeOperation cop
	         on cop.ChangeOperationID = dat.ChangeOperationID
	 left  join CAV22.dbo.TreatmentCodes tmc
	         on (case when len(tmc.Code) = 5 then concat(9, tmc.Code) else tmc.Code end) = ProcedureID
	 left  join CAV22.dbo.[Procedures] prc
	         on prc.Id = dat.ProcedureID
	 left  join CAV22.dbo.ProcedureCategories ptg
	         on ptg.Id = prc.ProcedureCategory_Id
where dat.ConfigChangeBuildID = @build_id


select EntityTypeName, EntityID, EntityName, ProcedureCategory, TreatmentCode, TreatmentName, count(*) as [count]
from #Build_Detail
group by EntityTypeName, EntityID, EntityName, ProcedureCategory, TreatmentCode, TreatmentName

drop table if exists #Build_Detail

end

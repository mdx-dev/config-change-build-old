use DataStage
go

create or alter procedure ccb.build_summary_counts @build_id int as
begin

set nocount on

--declare @build_id int = 6
declare @build_name nvarchar(max) = (select BuildName from DataStage.ccb.ConfigChangeBuild where ConfigChangeBuildID = @build_id)

drop table if exists #Build_Summary
select dat.EntityTypeID
     , EntityTypeName
	 , EntityID
	 , EntityName
	 , ProcedureID
	 , tmc.Code as TreatmentCode
	 , tmc.[Name] as TreatmentName
	 , dat.ConfigID
	 , ConfigName
	 , ConfigValueNew
	 , ConfigValueOld
	 , cop.ChangeOperationID
	 , ChangeOperationName
into #Build_Summary
from DataStage.ccb.ConfigChangeBuildSummary dat
     inner join DataStage.ccb.Config con
	         on con.ConfigID = dat.ConfigID
	 inner join DataStage.ccb.EntityType ent
	         on ent.EntityTypeID = dat.EntityTypeID
	 inner join DataStage.ccb.ChangeOperation cop
	         on cop.ChangeOperationID = dat.ChangeOperationID
	 left  join CAV22.dbo.TreatmentCodes tmc
	         on (case when len(tmc.Code) = 5 then concat(9, tmc.Code) else tmc.Code end) = ProcedureID
where dat.ConfigChangeBuildID = @build_id


drop table if exists #Entity_Lists
select ROW_NUMBER() over(order by a.EntityTypeName, a.EntityID) as r
     , a.EntityTypeName, a.EntityID, a.EntityName, a.TotalCount, b.ChangeCount, c.NoChangeCount
into #Entity_Lists
from (
select distinct EntityTypeName, EntityID, EntityName, count(*) as TotalCount
from #Build_Summary
group by EntityTypeName, EntityID, EntityName
) a
left  join 
(
select distinct EntityTypeName, EntityID, EntityName, count(*) as ChangeCount
from #Build_Summary
where IsDifferent = 1
group by EntityTypeName, EntityID, EntityName
) b on a.EntityTypeName = b.EntityTypeName and a.EntityID = b.EntityID
left  join 
(
select distinct EntityTypeName, EntityID, EntityName, count(*) as NoChangeCount
from #Build_Summary
where IsDifferent = 0
group by EntityTypeName, EntityID, EntityName
) c on a.EntityTypeName = c.EntityTypeName and a.EntityID = c.EntityID

declare @i int
declare @entity_type_name varchar
declare @entity_id varchar
declare @entity_name varchar
declare @total_count varchar
declare @change_count varchar
declare @no_change_count varchar
/*
print(N'Change Counts')
print(N'Entity Type' + char(9) + N'EntityID' + char(9) + N'Entity Name' + char(9) + N'Total Count' + char(9) + N'Change Count' + char(9) + N'No Change Count')
set @i = 1
while @i <= (select max(r) from #Entity_Lists)
begin
	set @entity_type_name = (select EntityTypeName from #Entity_Lists where r = @i)
	set @entity_id = cast((select EntityID from #Entity_Lists where r = @i) as varchar)
	set @entity_name = (select EntityName from #Entity_Lists where r = @i)
	set @total_count = cast((select TotalCount from #Entity_lists where r = @i) as varchar)
	set @change_count = cast((select ChangeCount from #Entity_Lists where r = @i) as varchar)
	set @no_change_count = cast((select NoChangeCount from #Entity_Lists where r = @i) as varchar)
	print(@entity_type_name + char(9) + @entity_id + char(9) + @entity_name + char(9) + @total_count + char(9) + @change_count + char(9) + @no_change_count)
	set @i += 1
end
*/

select *
from #Entity_Lists

end
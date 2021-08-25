USE DataStage;
GO

create or alter procedure ccb.validate_incentive_import @import_table_name nvarchar(255) as
begin

SET NOCOUNT ON

print ( char(13) +
       N'------------------------------------------------------------------------------------------' + char(13) +
       N'------------------------------ INCENTIVE IMPORT VALIDATIONS ------------------------------' + char(13) +
	   N'------------------------------------------------------------------------------------------' + char(13))

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
print @import_table_full_print_string + char(13)


drop table if exists #Required_Column_List
create table #Required_Column_List (required_columns varchar(255))
insert into #Required_Column_List
values ('Hierarchy'), ('Client Name'), ('Plan Name'), ('Treatment Category'), ('Treatment Code'), ('Treatment Name'),
       ('DI Savings'), ('DI Max Incentive'), ('DI Min Incentive'), ('Static Tier 1'), ('Static Tier 2'), ('Static Tier 3'),
	   ('Incentive Type'), ('Modify Type'), ('Ticket'), ('Note')

drop table if exists #Imported_Column_List
create table #Imported_Column_List (imported_columns varchar(255))


exec(
'insert into #Imported_Column_List (imported_columns)
select c.[Name] as imported_columns
from ' + @import_database + '.sys.tables t
     inner join ' + @import_database + '.sys.columns c
	         on t.object_id = c.object_id
where t.[Name] = ' + '''' + @import_table + '''' + ' and schema_name() = ' + '''' + @import_schema + ''''
)

drop table if exists #Column_Validations
create table #Column_Validations (required_columns varchar(255), imported_columns varchar(255), column_status varchar(255))

insert into #Column_Validations
select required_columns
     , imported_columns
	 , case when required_columns = imported_columns then 'match'
	        when required_columns is null then 'extra'
			when imported_columns is null then 'missing'
			else null
		end as column_status
from #Required_Column_List nc
full outer join #Imported_Column_List ic
             on ic.Imported_Columns = nc.Required_Columns

drop table if exists #Missing_Column_List
create table #Missing_Column_List (i int identity(1,1), missing_column nvarchar(255))
insert into #Missing_Column_List (missing_column) select required_columns from #Column_Validations where column_status = 'missing'

drop table if exists #Extra_Column_List
create table #Extra_Column_List (i int identity(1,1), extra_column nvarchar(255))
insert into #Extra_Column_List (extra_column) select imported_columns from #Column_Validations where column_status = 'extra'

drop table if exists #Match_Column_List
create table #Match_Column_List (i int identity(1,1), match_column nvarchar(255))
insert into #Match_Column_List (match_column) select required_columns from #Column_Validations where column_status = 'match'


declare @missing_column_count int = (select count(*) from #Column_Validations where column_status = 'missing')
declare @extra_column_count int   = (select count(*) from #Column_Validations where column_status = 'extra')
declare @match_column_count int   = (select count(*) from #Column_Validations where column_status = 'match')
declare @column_name nvarchar(255)
declare @i int

print (N'----------------------------------- COLUMN VALIDATIONS -----------------------------------')

print(N'Extra Column Count : ' + cast(@extra_column_count as nvarchar(255)))
set @i = 1
while @i <= @extra_column_count
begin
	set @column_name = (select extra_column from #Extra_Column_List where i = @i)
	print(char(9) + @column_name)
	set @i += 1
end

print(N'Missing Column Count : ' + cast(@missing_column_count as nvarchar(255)))
set @i = 1
while @i <= @extra_column_count
begin
	set @column_name = (select missing_column from #Missing_Column_List where i = @i)
	print(char(9) + @column_name)
	set @i += 1
end


if @missing_column_count > 0
begin
    print N'WARNING - REQUIRED COLUMNS NOT PRESENT - TERMINATING STORED PROCEDURE'
    GOTO END_PROCEDURE
end
else
begin
    print N'All required columns are present.' + char(13)
end



-- VALUE VALIDATIONS
drop table if exists #Incentives_Import_Stage
create table #Incentives_Import_Stage (
        [RowNumber]            int identity(1,1)
       ,[Hierarchy]            nvarchar(255)
       ,[Client Name]          nvarchar(255)
       ,[Plan Name]            nvarchar(255)
       ,[Treatment Category]   nvarchar(255)
       ,[Treatment Code]	   nvarchar(255)
       ,[Treatment Name]	   nvarchar(255)
       ,[DI Savings]		   nvarchar(255)
       ,[DI Max Incentive]	   nvarchar(255)
       ,[DI Min Incentive]	   nvarchar(255)
       ,[Static Tier 1]		   nvarchar(255)
       ,[Static Tier 2]		   nvarchar(255)
       ,[Static Tier 3]		   nvarchar(255)
       ,[Incentive Type]	   nvarchar(255)
       ,[Modify Type]		   nvarchar(255)
       ,[Ticket]			   nvarchar(255)
       ,[Note]				   nvarchar(255)
)


exec ('insert into #Incentives_Import_Stage ([Hierarchy], [Client Name], [Plan Name], [Treatment Category], [Treatment Code], [DI Savings]
                         , [DI Max Incentive], [DI Min Incentive], [Static Tier 1], [Static Tier 2], [Static Tier 3]
						 , [Incentive Type], [Modify Type], Ticket, Note)
select [Hierarchy], [Client Name], [Plan Name], [Treatment Category], [Treatment Code], [DI Savings]
     , [DI Max Incentive], [DI Min Incentive], [Static Tier 1], [Static Tier 2], [Static Tier 3]
     , [Incentive Type], [Modify Type], Ticket, Note
from ' + @import_table_name)

declare @import_row_count_total int = (select count(*) from #Incentives_Import_Stage)
declare @import_row_count_kept int = (select count(*) from #Incentives_Import_Stage where [Modify Type] is not null)
declare @import_row_count_removed int = (select count(*) from #Incentives_Import_Stage where [Modify Type] is null)

print(N'Initial Record Count: ' + cast(@import_row_count_total as varchar))
print(N'Empty Record Count: ' + cast(@import_row_count_removed as varchar))
print(N'Kept Record Count: ' + cast(@import_row_count_kept as varchar))


drop table if exists #Error_Table
select
       dat.*
     , case when dat.[Client Name] is not null and dat.[Client Name] = cln.[Name] then 1 else 0
	         end as v_Client
	 , case when (Hierarchy in ('Plan', 'Plan Treatment') and dat.[Plan Name] is not null and dat.[Plan Name] = pln.[Name])
	          or (Hierarchy in ('Client', 'Client Treatment') and dat.[Plan Name] is null) then 1 else 0
			 end as v_Plan
	 , case when (Hierarchy in ('Client Treatment', 'Plan Treatment') and dat.[Treatment Code] is not null and dat.[Treatment Code] = tmc.Code)
	          or (Hierarchy in ('Client', 'Plan') and dat.[Treatment Code] is null) then 1 else 0
			 end as v_Treatment
	 , case when dat.Hierarchy in ('Client', 'Plan', 'Client Treatment', 'Plan Treatment') and dat.Hierarchy is not null then 1 else 0
	         end as v_Hierarchy
	 , case when [Modify Type] in ('update', 'insert', 'delete') then 1 else 0
	         end as v_Modify
	 , case when [Incentive Type] in ('Dynamic', 'Static', 'CTO') then 1 else 0
	         end as v_IncentiveType
	 , case when Ticket is not null then 1 else 0
	         end as v_Ticket
	 , case when (isnumeric([DI Savings]) = 1 and [DI Savings] not like '%.%' and cast([DI Savings] as int) between 0 and 100)
	          or [DI Savings] is null then 1 else 0
	         end as v_Savings
	 , case when (isnumeric([DI Max Incentive]) = 1 and [DI Max Incentive] not like '%.%' and cast([DI Max Incentive] as int) >= 0)
	          or [DI Max Incentive] is null then 1 else 0
	         end as v_Max
	 , case when (isnumeric([DI Min Incentive]) = 1 and [DI Min Incentive] not like '%.%' and cast([DI Min Incentive] as int) >= 0)
	          or [DI Min Incentive] is null then 1 else 0
	         end as v_Min
	 , case when (isnumeric([Static Tier 1]) = 1 and [Static Tier 1] >= 0)
	          or [Static Tier 1] is null then 1 else 0
	         end as v_Tier1
	 , case when (isnumeric([Static Tier 2]) = 1 and [Static Tier 2] >= 0)
	          or [Static Tier 2] is null then 1 else 0
	         end as v_Tier2
	 , case when (isnumeric([Static Tier 3]) = 1 and [Static Tier 3] >= 0)
	          or [Static Tier 3] is null then 1 else 0
	         end as v_Tier3
	 , case when [Incentive Type] not in ('CTO', 'Dynamic', 'Static')
	          or ([Incentive Type] = 'CTO'
	              and [DI Savings] is null
				  and [DI Max Incentive] is null
				  and [DI Min Incentive] is null  
				  and [Static Tier 1] is null
				  and [Static Tier 2] is null
				  and [Static Tier 3] is null)
			 or ([Incentive Type] = 'Dynamic'
			      and isnumeric([DI Savings]) = 1
				  and isnumeric([DI Max Incentive]) = 1
				  and isnumeric([DI Min Incentive]) = 1
				  and [DI Savings] not like '%.%'
				  and [DI Max Incentive] not like '%.%'
				  and [DI Min Incentive] not like '%.%'
				  and (cast([DI Max Incentive] as int) > cast([DI Min Incentive] as int))
				  and [Static Tier 1] is null
				  and [Static Tier 2] is null
				  and [Static Tier 3] is null)
			or ([Incentive Type] = 'Static'
				 and [DI Savings] is null
				 and [DI Max Incentive] is null
				 and [DI Min Incentive] is null
				 and isnumeric([Static Tier 1]) = 1
				 and (isnumeric([Static Tier 2]) = 1 or [Static Tier 2] is null)
				 and (isnumeric([Static Tier 3]) = 1 or [Static Tier 3] is null))
			then 1
			else 0
             end as v_Incentive
into #Error_Table
from #Incentives_Import_Stage dat
     left  join CAV22.dbo.Clients cln
	         on cln.[Name] = dat.[Client Name]
			and cln.IsActive = 1
	 left  join CAV22.dbo.Plans pln
	         on pln.[Name] = dat.[Plan Name]
			and pln.IsActive = 1
	 left  join CAV22.dbo.TreatmentCodes tmc
	         on tmc.Code = dat.[Treatment Code]
where dat.[Modify Type] is not null

select * from #Incentives_Import_Stage
select RowNumber, ErrorType
from (
select RowNumber
     , case when ErrorType = 'v_client' then 'Client'
	        when ErrorType = 'v_Plan' then 'Plan'
			when ErrorType = 'v_Treatment' then 'Treatment'
			when ErrorType = 'v_Hierarchy' then 'Hierarchy'
			when ErrorType = 'v_Modify' then 'Modify Type'
			when ErrorType = 'v_IncentiveType' then 'Incentive Type'
			when ErrorType = 'v_Ticket' then 'Ticket'
			when ErrorType = 'v_Savings' then 'DI Savings'
			when ErrorType = 'v_Max' then 'DI Max Incentive'
			when ErrorType = 'v_Min' then 'DI Min Incentive'
			when ErrorType = 'v_Tier1' then 'Static Tier 1'
			when ErrorType = 'v_Tier2' then 'Static Tier 2'
			When ErrorType = 'v_Tier3' then 'Static Tier 3'
			when ErrorType = 'v_Incentive' then 'Incentive Type Parameters'
			else 'NO ERROR EXISTS'
	 end as ErrorType
	, Valid
from #Error_Table
unpivot
(
Valid
for ErrorType in (v_client, v_plan, v_Treatment, v_Hierarchy, v_Modify, v_IncentiveType, v_Ticket, v_Savings, v_Max, v_Min, v_Tier1, v_Tier2, v_Tier3, v_Incentive)
) a
) a
where Valid = 0
order by RowNumber, ErrorType
END_PROCEDURE:
print(char(13) + char(13) + N'END STORED PROCEDURE')
end
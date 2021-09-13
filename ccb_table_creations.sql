use DataStage
go

--create schema ccb

drop table if exists ccb.ConfigChangeBuild
create table ccb.ConfigChangeBuild (
    ConfigChangeBuildID int identity(1,1) not null primary key,
	BuildName nvarchar(255) not null,
	IsDeployed bit default(0) not null,
	DateLastDeployed datetime null,
	DateLastRolledBack datetime null,
	DateAdded datetime default(getdate()) not null,
	AddedBy varchar(100) default(user) not null,
	DateModified datetime null,
	ModifiedBy nvarchar(100) null,
	ModifiedVersion smallint default(0) not null,
	ModifiedReason nvarchar(255) null)

drop table if exists ccb.ConfigChangeBuildSummary
create table ccb.ConfigChangeBuildSummary (
    ConfigChangeBuildSummaryID int identity(1,1) not null primary key,
	ConfigChangeBuildID int not null,
	EntityTypeID tinyint not null,
	EntityID bigint not null,
	EntityName nvarchar(255),
	ProcedureID bigint null,
	ConfigID int not null,
	ConfigValueNew nvarchar(255) null,
	ConfigValueOld nvarchar(255) null,
	ChangeOperationID tinyint not null,
	Ticket varchar(20) not null)

drop table if exists ccb.EntityType 
create table ccb.EntityType (
    EntityTypeID smallint identity(1,1) not null primary key,
	EntityTypeName nvarchar(255) not null,
	EntityTypeDisplayName nvarchar(255) null,
	EntityDescription nvarchar(255) null,
	DateAdded datetime default(getdate()) not null,
	AddedBy nvarchar(100) default(user) not null,
	DateModified datetime null,
	ModifiedBy nvarchar(100) null,
	ModifiedVersion smallint default(0) not null,
	ModifiedReason nvarchar(255) null)

insert into ccb.EntityType (EntityTypeName, EntityTypeDisplayName)
values (('client'), ('Client')),
       (('plan'), ('Plan')),
	   (('employer'), ('Employer')),
	   (('employer_group'), ('Employer Group')),
	   (('treatment_client'), ('Client Treatment')),
	   (('treatment_plan'), ('Plan Treatment')),
	   (('treatment_employer'), ('Employer Treatment')),
	   (('treatment_employer_group'), ('Employer Group Treatment')),
	   (('default'), ('Default'))

drop table if exists ccb.ChangeOperation 
create table ccb.ChangeOperation (
    ChangeOperationID tinyint identity(1,1) not null primary key,
	ChangeOperationName nvarchar(255) not null,
	DateAdded datetime default(getdate()) not null,
	AddedBy nvarchar(100) default(user) not null,
	DateModified datetime null,
	ModifiedBy nvarchar(100) null,
	ModifiedVersion smallint default(0) not null,
	ModifiedReason nvarchar(255) null)

insert into ccb.ChangeOperation (ChangeOperationName)
values ('update'), ('insert'), ('delete')


drop table if exists ccb.Config
create table ccb.Config (
    ConfigID smallint identity(1,1) not null primary key,
	ConfigName nvarchar(255) not null,
	ConfigGroup nvarchar(255) not null,
	ConfigSubGroup nvarchar(255),
	ConfigDescription nvarchar(255),
	DateAdded datetime default(getdate()) not null,
	AddedBy nvarchar(100) default(user) not null,
	DateModified datetime null,
	ModifiedBy nvarchar(100) null,
	ModifiedVersion smallint default(0) not null,
	ModifiedReason nvarchar(255) null)

insert into ccb.Config (ConfigName, ConfigGroup, ConfigSubGroup)
values (('show_incentives'), ('configuration'), ('incentives')),
       (('provider_type'), ('configuration'), ('incentives')),
	   (('minimum_incentive_amount'), ('configuration'), ('incentives')),
	   (('maximum_incentive_amount'), ('configuration'), ('incentives')),
	   (('percentage_of_savings'), ('configuration'), ('incentives')),
       (('static_tier_1'), ('incentive_amounts'), null),
       (('static_tier_2'), ('incentive_amounts'), null),
       (('static_tier_3'), ('incentive_amounts'), null),
	   (('treatment_code'), ('treatment_parameter'), null),
	   (('min_radius'), ('treatment_parameter'), null),
	   (('x12_code'), ('treatment_parameter'), null),
	   (('alternate_name'), ('treatment_parameter'), null),
	   (('use_ncct_description'), ('treatment_parameter'), null),
	   (('super_type'), ('treatment_parameter'), null),
	   (('treatment_code_external'), ('treatment_parameter'), null),
	   (('use_surgical_concierge'), ('treatment_parameter'), null),
	   (('is_rts'), ('treatment_parameter'), null),
	   (('rts_category'), ('treatment_parameter'), null),
	   (('is_suppressed'), ('treatment_parameter'), ('treatment_plan'))
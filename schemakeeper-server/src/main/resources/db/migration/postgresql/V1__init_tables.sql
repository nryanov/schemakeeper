create schema if not exists ${schemakeeper_schema};

create table if not exists ${schemakeeper_schema}.schema_type (
  schema_type_name varchar(255) not null,
  constraint schema_type_pk primary key (schema_type_name)
);

create table if not exists ${schemakeeper_schema}.compatibility_type (
  compatibility_type_name varchar(255) not null,
  constraint compatibility_type_pk primary key (compatibility_type_name)
);

create table if not exists ${schemakeeper_schema}.config (
  config_name varchar(255) not null,
  config_value text not null,
  constraint configs_pk primary key (config_name)
);

create table if not exists ${schemakeeper_schema}.subject (
  subject_name varchar(255) not null,
  schema_type_name varchar(255) not null,
  compatibility_type_name varchar(255) not null,
  constraint subject_pk primary key (subject_name),
  constraint subject_schema_type_fk foreign key (schema_type_name) references ${schemakeeper_schema}.schema_type (schema_type_name) on delete restrict on update restrict,
  constraint subject_compatibility_type_fk foreign key (compatibility_type_name) references ${schemakeeper_schema}.compatibility_type (compatibility_type_name) on delete restrict on update restrict
);

create table if not exists ${schemakeeper_schema}.schema_info (
  id serial,
  version int not null,
  schema_type_name varchar(255) not null,
  subject_name varchar(255) not null,
  schema_text text not null,
  schema_hash char(32) not null,
  constraint schema_info_pk primary key (id),
  constraint schema_info_subject_fk foreign key (subject_name) references ${schemakeeper_schema}.subject (subject_name) on delete cascade on update cascade,
  constraint schema_type_fk foreign key (schema_type_name) references ${schemakeeper_schema}.schema_type (schema_type_name) on delete restrict on update restrict,
  constraint schema_info_schema_hash_unique unique (schema_hash)
);

create index if not exists schemakeeper_schema_info_fk_idx on ${schemakeeper_schema}.schema_info (subject_name);
create index if not exists schemakeeper_schema_info_type_subject_version_idx on ${schemakeeper_schema}.schema_info (schema_type_name, subject_name, version);

insert into ${schemakeeper_schema}.config(config_name, config_value) values('default.compatibility', 'backward');

insert into ${schemakeeper_schema}.schema_type(schema_type_name) values ('avro');
insert into ${schemakeeper_schema}.schema_type(schema_type_name) values ('thrift');
insert into ${schemakeeper_schema}.schema_type(schema_type_name) values ('protobuf');

insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('none');
insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('backward');
insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('forward');
insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('full');
insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('backward_transitive');
insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('forward_transitive');
insert into ${schemakeeper_schema}.compatibility_type(compatibility_type_name) values ('full_transitive');

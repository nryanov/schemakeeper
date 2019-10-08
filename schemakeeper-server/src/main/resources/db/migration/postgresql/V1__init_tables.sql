create schema if not exists ${schemakeeper_schema};

create table if not exists ${schemakeeper_schema}.schema_type (
  schema_type_name varchar(255) not null,
  constraint schema_type_pk primary key (schema_type_name)
);

create table if not exists ${schemakeeper_schema}.compatibility_type (
  compatibility_type_name varchar(255) not null,
  constraint compatibility_type_pk primary key (compatibility_type_name)
);

create table if not exists ${schemakeeper_schema}.subject (
  subject_name varchar(255) not null,
  compatibility_type_name varchar(255) not null,
  is_locked boolean not null default false,
  creation_ts timestamp not null default now(),
  constraint subject_pk primary key (subject_name),
  constraint subject_compatibility_type_fk foreign key (compatibility_type_name) references ${schemakeeper_schema}.compatibility_type (compatibility_type_name) on delete restrict on update restrict
);

create table if not exists ${schemakeeper_schema}.schema_info (
  schema_id serial,
  schema_type_name varchar(255) not null,
  schema_text text not null,
  schema_hash char(32) not null,
  creation_ts timestamp not null default now(),
  constraint schema_info_pk primary key (schema_id),
  constraint schema_type_fk foreign key (schema_type_name) references ${schemakeeper_schema}.schema_type (schema_type_name) on delete restrict on update restrict,
  constraint schema_info_schema_hash_unique unique (schema_hash)
);

create table if not exists ${schemakeeper_schema}.subject_schema (
  subject_name varchar(255) not null,
  schema_id int not null,
  version int not null,
  creation_ts timestamp not null default now(),
  constraint subject_schema_pk primary key (subject_name, schema_id),
  constraint subject_schema_version_unique unique (subject_name, version),
  constraint subject_fk foreign key (subject_name) references ${schemakeeper_schema}.subject (subject_name) on delete cascade on update cascade,
  constraint schema_fk foreign key (schema_id) references ${schemakeeper_schema}.schema_info (schema_id) on delete cascade on update cascade
);

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

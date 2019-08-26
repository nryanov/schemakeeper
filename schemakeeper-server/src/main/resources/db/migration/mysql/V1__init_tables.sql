create schema if not exists schemakeeper;

create table if not exists schemakeeper.schema_type (
  schema_type_name varchar(255) not null,
  constraint schema_type_pk primary key (schema_type_name)
);

create table if not exists schemakeeper.compatibility_type (
  compatibility_type_name varchar(255) not null,
  constraint compatibility_type_pk primary key (compatibility_type_name)
);

create table if not exists schemakeeper.subject (
  subject_name varchar(255) not null,
  schema_type_name varchar(255) not null,
  compatibility_type_name varchar(255) not null,
  constraint subject_pk primary key (subject_name),
  constraint subject_schema_type_fk foreign key (schema_type_name) references schemakeeper.schema_type (schema_type_name) on delete restrict on update restrict,
  constraint subject_compatibility_type_fk foreign key (compatibility_type_name) references schemakeeper.compatibility_type (compatibility_type_name) on delete restrict on update restrict
);

create table if not exists schemakeeper.schema_info (
  id int auto_increment,
  version int not null,
  schema_type_name varchar(255) not null,
  subject_name varchar(255) not null,
  schema_text text not null,
  schema_hash char(32) not null,
  constraint schema_info_pk primary key (id),
  constraint schema_info_subject_fk foreign key (subject_name) references schemakeeper.subject (subject_name) on delete cascade on update cascade,
  constraint schema_type_fk foreign key (schema_type_name) references schemakeeper.schema_type (schema_type_name) on delete restrict on update restrict,
  constraint schema_info_schema_hash_unique unique (schema_hash)
);

alter table schemakeeper.schema_info add index (subject_name);
alter table schemakeeper.schema_info add index (schema_type_name, subject_name, version);

insert into schemakeeper.schema_type(schema_type_name) values ('avro');

insert into schemakeeper.compatibility_type(compatibility_type_name) values ('none');
insert into schemakeeper.compatibility_type(compatibility_type_name) values ('backward');
insert into schemakeeper.compatibility_type(compatibility_type_name) values ('forward');
insert into schemakeeper.compatibility_type(compatibility_type_name) values ('full');
insert into schemakeeper.compatibility_type(compatibility_type_name) values ('backward_transitive');
insert into schemakeeper.compatibility_type(compatibility_type_name) values ('forward_transitive');
insert into schemakeeper.compatibility_type(compatibility_type_name) values ('full_transitive');

create table if not exists schema_type (
  schema_type_name varchar(255) not null,
  constraint schema_type_pk primary key (schema_type_name)
);

create table if not exists compatibility_type (
  compatibility_type_name varchar(255) not null,
  constraint compatibility_type_pk primary key (compatibility_type_name)
);

create table if not exists subject (
  subject_name varchar(255) not null,
  schema_type_name varchar(255) not null,
  compatibility_type_name varchar(255) not null,
  constraint subject_pk primary key (subject_name),
  constraint subject_schema_type_fk foreign key (schema_type_name) references schema_type (schema_type_name) on delete restrict on update restrict,
  constraint subject_compatibility_type_fk foreign key (compatibility_type_name) references compatibility_type (compatibility_type_name) on delete restrict on update restrict
);

create table if not exists schema_info (
  id serial,
  version int not null,
  schema_type_name varchar(255) not null,
  subject_name varchar(255) not null,
  schema_text text not null,
  schema_hash char(32) not null,
  constraint schema_info_pk primary key (id),
  constraint schema_info_subject_fk foreign key (subject_name) references subject (subject_name) on delete cascade on update cascade,
  constraint schema_type_fk foreign key (schema_type_name) references schema_type (schema_type_name) on delete restrict on update restrict,
  constraint schema_info_schema_hash_unique unique (schema_hash)
);

create index if not exists schema_info_subject_name_idx on schema_info(subject_name);
create index if not exists schema_info_meta_idx on schema_info(schema_type_name, subject_name, version);

insert into schema_type(schema_type_name) values ('avro');
insert into schema_type(schema_type_name) values ('thrift');
insert into schema_type(schema_type_name) values ('protobuf');

insert into compatibility_type(compatibility_type_name) values ('none');
insert into compatibility_type(compatibility_type_name) values ('backward');
insert into compatibility_type(compatibility_type_name) values ('forward');
insert into compatibility_type(compatibility_type_name) values ('full');
insert into compatibility_type(compatibility_type_name) values ('backward_transitive');
insert into compatibility_type(compatibility_type_name) values ('forward_transitive');
insert into compatibility_type(compatibility_type_name) values ('full_transitive');

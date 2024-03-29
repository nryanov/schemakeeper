# Schemakeeper
[![GitHub license](https://img.shields.io/github/license/nryanov/Schemakeeper)](https://github.com/nryanov/Schemakeeper/blob/master/LICENSE.txt)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.nryanov.schemakeeper/schemakeeper-common/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.nryanov.schemakeeper/schemakeeper-common)
[![Codecov](https://img.shields.io/codecov/c/github/nryanov/Schemakeeper)](https://img.shields.io/codecov/c/github/nryanov/Schemakeeper)
[![Schemakeeper CI](https://github.com/nryanov/Schemakeeper/actions/workflows/scala.yml/badge.svg?branch=master)](https://github.com/nryanov/Schemakeeper/actions/workflows/scala.yml)

Schemakeeper - yet another schema registry for Avro, Thrift and Protobuf schemas. 
It provides a RESTful interface for storing and retrieving Subjects and Schemas metadata.

## Features
- Allows to store metadata for Avro, Thrift and Protobuf schemas
- Allows to use PostgreSQL, MySQL, H2, MariaDB or Oracle as backend for server
- Can easily be used not only with Kafka 
- Rich REST api
- Swagger doc & swagger ui
- More control on existing subjects - you can lock it to prevent adding new schemas to this subject

### Oracle notes
To be able to use Oracle as your backend for schemas it is required to install ojdbc jar manually. You can do it using maven (or any other tool):
```text
mvn install:install-file -Dfile=path/to/your/ojdbc8.jar -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=19.3.0.0 -Dpackaging=jar
```

## Requirements
- Java 8+

## Modules
- [`schemakeeper-common`](schemakeeper-common) - common classes for SerDe's and API
- [`schemakeeper-client`](schemakeeper-client) - http client implementation
- [`schemakeeper-avro`](schemakeeper-avro) - Avro SerDe
- [`schemakeeper-thrift`](schemakeeper-thrift) - Thrift SerDe
- [`schemakeeper-protobuf`](schemakeeper-protobuf) - Protobuf SerDe
- [`schemakeeper-kafka-common`](schemakeeper-kafka-common) - common classes for Kafka SerDe
- [`schemakeeper-kafka-avro`](schemakeeper-kafka-avro) - Avro SerDe for Kafka
- [`schemakeeper-kafka-thrift`](schemakeeper-kafka-thrift) - Thrift SerDe for Kafka
- [`schemakeeper-kafka-protobuf`](schemakeeper-kafka-protobuf) - Protobuf SerDe for Kafka

## Installation
Every Schemakeeper module is published at Maven Central

```xml
<dependency>
  <groupId>com.nryanov.schemakeeper</groupId>
  <artifactId>schemakeeper-${module.name}</artifactId>
  <version>${module.version}</version>
</dependency>
```

```groovy
compile 'com.nryanov.schemakeeper:${module.name}:${module.version}'
```

```sbt
"com.nryanov.schemakeeper" %% "<schemakeeper-module>" % "[version]"
```

## Build
Server build:
```bash
sbt server/stage
// executable should be in server/target/universal/stage/bin/*
```
### Docker
```bash
sbt server/docker:publishLocal
```

## Test
```bash
sbt server/docker:publishLocal
sbt test
```

## Run
```bash
docker pull nryanov/schemakeeper:{version}
docker run --name={container_name} -p 9081:9081 -p 9990:9990 -d nryanov/schemakeeper:{version}
```

## Settings
Server uses [Lightbend HOCON config](https://github.com/lightbend/config) for configuration.
By default server is listening **9081** port for rest. Also, H2 is used as default server backend with next settings:

```hocon
schemakeeper {
  server {
    port = 9081
    host = 0.0.0.0
  }

  storage {
    username = ""
    password = ""
    driver = "org.h2.Driver"
    schema = "public"
    url = "jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1"
  }
}
```

### CORS
```hocon
schemakeeper {
  server {
    cors {
      allowedCredentials = "<true/false>"
      anyOrigin = "<true/false>"
      anyMethod = "<true/false>"
      maxAge = "<number>"
      allowsOrigins = "<ORIGIN>"
      allowsMethods = "<Comma-separated methods>"
      allowsHeaders = "<Comma-separated headers>"
      exposedHeaders = "<Comma-separated headers>"
    }
  }
}
```

### Jar
If you using jar for starting server, you can configure your app using **-Dconfig.file** java option: 
```bash
java -jar -Dconfig.file=<PATH TO application.conf> schemakeeper-server.jar
```

### Docker
To configure docker image you can use environment variables:
- SCHEMAKEEPER_LISTENING_PORT - listening port for rest api
- SCHEMAKEEPER_LISTENING_HOST - host
- SCHEMAKEEPER_STORAGE_USERNAME - db username
- SCHEMAKEEPER_STORAGE_PASSWORD - db password
- SCHEMAKEEPER_STORAGE_DRIVER - driver (org.h2.Driver, com.mysql.jdbc.Driver, org.postgresql.Driver, org.mariadb.jdbc.Driver, oracle.jdbc.driver.OracleDriver)
- SCHEMAKEEPER_STORAGE_SCHEMA - db schema
- SCHEMAKEEPER_STORAGE_URL - jdbc connection url

**Cors settings:**
- SCHEMAKEEPER_ALLOWS_ORIGINS - Allowed origins
- SCHEMAKEEPER_ALLOWS_METHODS - Comma-separated methods
- SCHEMAKEEPER_ALLOWS_HEADERS - Comma-separated headers

## Usage

### Client settings
Under the hood the [Unirest](https://github.com/Kong/unirest-java) is used as http client. You can configure it:
```java
Map<String, Object> properties = new HashMap();
properties.put(ClientConfig.CLIENT_MAX_CONNECTIONS, 10);
properties.put(ClientConfig.CLIENT_CONNECTIONS_PER_ROUTE, 5);
properties.put(ClientConfig.CLIENT_SOCKET_TIMEOUT, 5000);
properties.put(ClientConfig.CLIENT_CONNECT_TIMEOUT, 5000);

// Optionally, you can confgure your http client to use proxy
properties.put(ClientConfig.CLIENT_PROXY_HOST, "proxyHost");
properties.put(ClientConfig.CLIENT_PROXY_PORT, port);
properties.put(ClientConfig.CLIENT_PROXY_USERNAME, "username");
properties.put(ClientConfig.CLIENT_PROXY_PASSWORD, "password");
```  

### Avro
```java
Map<String, Object> properties = new HashMap();
properties.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "url");
AvroSerDeConfig config = new AvroSerDeConfig(properties);
AvroSerializer serializer = new AvroSerilizer(config);
AvroDeserializer deserializer = new AvroDeserializer(config);

byte[] b = serializer.serialize("subject", data);
Object r = deserializer.deserialize(b);
```

Avro SerDe also compatible with Thrift and Protobuf serialized data. It is possible to serialize Thrift/Protobuf using Thrift/Protobuf serializer and then deserialize it using AvroDeserializer.

**With Kafka**

Use `KafkaAvroSerializer.class` and `KafkaAvroDeserializer.class`.
Also, the required property is `SerDeConfig.SCHEMAKEEPER_URL_CONFIG`. Other settings are optional for SerDe.

### Thrift
Before using Thrift SerDe, you should generate Thrift classes.
```java
Map<String, Object> properties = new HashMap();
properties.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "url");
ThriftSerDeConfig config = new ThriftSerDeConfig(properties);
ThriftSerializer serializer = new ThriftSerializer(config);
ThriftDeserializer deserializer = new ThriftDeserializer(config);

byte[] b = serializer.serialize("subject", data);
Object r = deserializer.deserialize(b);
```

**With Kafka**

Use `KafkaThriftSerializer.class` and `KafkaThriftDeserializer.class`.
Also, the required property is `SerDeConfig.SCHEMAKEEPER_URL_CONFIG`. Other settings are optional for SerDe.
### Protobuf
Before using Protobuf SerDe, you should generate Protobuf classes.
```java
Map<String, Object> properties = new HashMap();
properties.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "url");
ProtobufSerDeConfig config = new ProtobufSerDeConfig(properties);
ProtobufSerializer serializer = new ProtobufSerializer(config);
ProtobufDeserializer deserializer = new ProtobufDeserializer(config);

byte[] b = serializer.serialize("subject", data);
Object r = deserializer.deserialize(b);
```

**With Kafka**

Use `KafkaProtobufSerializer.class` and `KafkaProtobufDeserializer.class`.
Also, the required property is `SerDeConfig.SCHEMAKEEPER_URL_CONFIG`. Other settings are optional for SerDe.

## Swagger
Swagger DOC: **/docs**

## API
### subjects
**GET /v2/subjects**

Get list of registered subjects

**Response:**
- Json array of strings

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error

### subjectMetadata
**GET /v2/subjects/{subject_name}**

Get subject metadata by name

**Response:**
- Json object:
    - subject (string)
    - compatibilityType (string)
    - isLocked (boolean)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
    
### updateSubjectSettings
**PUT /v2/subjects/{subject_name}**

Update subject settings


**Body:**
```json
{
 "compatibilityType": "Compatibility type",
 "isLocked": "true/false"
}
```

**Response:**
- Json object:
    - subject (string)
    - compatibilityType (string)
    - isLocked (boolean)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
### subjectVersions
**GET /v2/subjects/{subject_name}/versions**

Get list of subject's schema versions

**Response:**
- Json array of ints

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
### subjectSchemasMetadata
**GET /v2/subjects/{subject_name}/schemas**

Get list of subject's schemas metadata

**Response:**
- Json array of objects:
    - schemaId (int)
    - version (int)
    - schemaText (string)
    - schemaHash (string)
    - schemaType (string)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
### subjectSchemaByVersion
**GET /v2/subjects/{subject_name}/versions/{version}**

Get subject's schema metadata by version

**Response:**
- Json object:
    - schemaId (int)
    - version (int)
    - schemaText (string)
    - schemaHash (string)
    - schemaType (string)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
    - Code 1004 Subject schema with such version does not exist
    
### schemaById
**GET /v2/schemas/{id}**

Get schema by id

**Response:**
- Json object:
    - schemaId (int)
    - schemaText (string)
    - schemaHash (string)
    - schemaType (string)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1005 Schema does not exist
### schemaIdBySubjectAndSchema
**POST /v2/subjects/{subject_name}/schemas/id**

**Body:**
```json
{
 "schemaText": "AVRO SCHEMA STRING",
 "schemaType": "IDENTIFIER Of SCHEMA TYPE [avro, thrift or protobuf]"
}
```

Check if schema is registered and connected with current subject and return it's id

**Response:**
- Json object:
    - schemaId (int)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1011 Schema is not registered
    - Code 1012 Schema is not connected to subject
- Bad request 400
    - Code 1007 Schema is not valid 
### deleteSubject
**DELETE /v2/subjects/{subject_name}**

Delete subject metadata

**Response:**
- Json boolean

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error

### deleteSubjectSchemaByVersion
**DELETE /v2/subjects/{subject_name}/versions/{version_number}**

Delete subject schema by version

**Response:**
- Json boolean

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
- Bad request 400
    - Code 1004 Subject schema with such version does not exist
### checkSubjectSchemaCompatibility
**POST /v2/subjects/{subject_name}/compatibility/schemas**

**Body:**
```json
{
 "schemaText": "AVRO SCHEMA STRING",
 "schemaType": "IDENTIFIER Of SCHEMA TYPE [avro, thrift or protobuf]"
}
```

Check schema compatibility 

**Response:**
- Json boolean

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Not found 404
    - Code 1001 Subject does not exist
- Bad request 400
    - Code 1007 Schema is not valid
    
### registerSchema
**POST /v2/schemas**

**Body:**
```json
{
 "schemaText": "AVRO SCHEMA STRING",
 "schemaType": "IDENTIFIER Of SCHEMA TYPE [avro, thrift or protobuf]"
}
```

Register new schema

**Response:**
- Json object:
    - schemaId (int)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Bad request 400
    - Code 1007 Schema is not valid
    - Code 1008 Schema is already exist

### registerSchemaAndSubject
**POST /v2/subjects/{subject_name}/schemas**

**Body:**
```json
{
 "schemaText": "AVRO SCHEMA STRING",
 "schemaType": "IDENTIFIER Of SCHEMA TYPE [avro, thrift or protobuf]",
 "compatibilityType": "SUBJECT COMPATIBILITY TYPE"
}
```

Register new subject (if not exists), schema (if not exists) and connect it to each other

**Response:**
- Json object:
    - schemaId (int)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Bad request 400
    - Code 1007 Schema is not valid
    - Code 1008 Schema is already exist
    - Code 1010 Schema is not compatible
    - Code 1013 Subject is locked
### registerSubject
**POST /v2/subjects**

**Body:**
```json
{
 "subject": "subject name",
 "compatibilityType": "compatibility type name",
 "isLocked": "false or true"
}
```

Register new subject

**Response:**
- Json object:
    - subject (string)
    - compatibilityType (string)
    - isLocked (boolean)

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Bad request 400
    - Code 1002 Subject is already exist
### addSchemaToSubject
**POST /v2/subjects/{subject_name}/schemas/{schema_id}**

Connect schema to subject as next version

**Response:**
- Json int - version number

**Status codes:**
- Internal server error 500 
    - Code 1000 Backend error
- Bad request 400
    - Code 1009 Schema is already connected to subject
    - Code 1013 Subject is locked
- Not found 404
    - Code 1001 Subject does not exist
    - Code 1005 Schema does not exist
    
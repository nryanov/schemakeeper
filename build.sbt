lazy val kindProjectorVersion = "0.13.0"
lazy val slf4jVersion = "1.7.25"
lazy val avroVersion = "1.9.0"
lazy val protobufVersion = "3.6.1"
lazy val thriftVersion = "0.12.0"
lazy val http4sVersion = "0.21.0-RC4"
lazy val tapirVersion = "0.12.20"
lazy val pureconfigVersion = "0.12.2"
lazy val enumeratumVersion = "1.5.13"
lazy val log4catsVersion = "1.0.1"
lazy val doobieVersion = "0.8.8"
lazy val flywayVersion = "6.0.1"
lazy val postgresqlDriverVersion = "42.2.6"
lazy val mysqlDriverVersion = "8.0.17"
lazy val mariadbDriverVersion = "2.5.4"
lazy val kafkaClientVersion = "2.0.0"

lazy val logbackVersion = "1.2.3"
lazy val scalatestVersion = "3.2.9"
lazy val testcontainersVersion = "0.35.0"
lazy val embeddedKafkaVersion = "2.1.0"

val scala2_12 = "2.12.13"

val compileAndTest = "compile->compile;test->test"

lazy val buildSettings = Seq(
  sonatypeProfileName := "com.nryanov",
  organization := "com.nryanov.schemakeeper",
  homepage := Some(url("https://github.com/nryanov/schemakeeper")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "nryanov",
      "Nikita Ryanov",
      "ryanov.nikita@gmail.com",
      url("https://nryanov.com")
    )
  ),
  scalaVersion := scala2_12
)

lazy val noPublish = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

def compilerOptions(scalaVersion: String) = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xlint",
  "-language:existentials",
  "-language:postfixOps"
) ++ (CrossVersion.partialVersion(scalaVersion) match {
  case Some((2, scalaMajor)) if scalaMajor == 12 => scala212CompilerOptions
  case Some((2, scalaMajor)) if scalaMajor == 13 => scala213CompilerOptions
})

lazy val scala212CompilerOptions = Seq(
  "-Yno-adapted-args",
  "-Ywarn-unused-import",
  "-Xfuture"
)

lazy val scala213CompilerOptions = Seq(
  "-Wunused:imports"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions(scalaVersion.value),
  addCompilerPlugin(
    ("org.typelevel" %% "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)
  ),
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-api" % slf4jVersion
  ),
  Test / parallelExecution := false
)

lazy val allSettings = commonSettings ++ buildSettings

lazy val schemakeeper =
  project
    .in(file("."))
    .settings(moduleName := "schemakeeper")
    .settings(allSettings)
    .settings(noPublish)
    .aggregate(
      )

lazy val common = project.in(file("modules/common")).settings(moduleName := "schemakeeper-common")
lazy val server = project.in(file("modules/server")).settings(moduleName := "schemakeeper-server")
lazy val client = project.in(file("modules/client")).settings(moduleName := "schemakeeper-client")
lazy val avro = project.in(file("modules/avro")).settings(moduleName := "schemakeeper-avro")
lazy val thrift = project.in(file("modules/thrift")).settings(moduleName := "schemakeeper-thrift")
lazy val protobuf = project.in(file("modules/protobuf")).settings(moduleName := "schemakeeper-protobuf")
lazy val kafkaCommon = project.in(file("modules/kafka/common")).settings(moduleName := "schemakeeper-kafka-common")
lazy val kafkaAvro = project.in(file("modules/kafka/avro")).settings(moduleName := "schemakeeper-kafka-avro")
lazy val kafkaThrift = project.in(file("modules/kafka/thrift")).settings(moduleName := "schemakeeper-kafka-thrift")
lazy val kafkaProtobuf =
  project.in(file("modules/kafka/protobuf")).settings(moduleName := "schemakeeper-kafka-protobuf")

//// common for all
//compile group: 'org.slf4j', name: 'slf4j-api', version: '1.7.25'
//compile group: 'org.apache.avro', name: 'avro', version: '1.9.0'
//
//testCompile group: 'ch.qos.logback', name: 'logback-classic', version: '1.2.3'
//testCompile group: 'org.junit.jupiter', name: 'junit-jupiter-api', version: '5.5.1'
//testCompile group: 'org.junit.jupiter', name: 'junit-jupiter-engine', version: '5.5.1'
//testCompile group: 'org.junit.vintage', name: 'junit-vintage-engine', version: '5.5.1'
//
//
//
//
//// server
//compile project(':schemakeeper-common')
//
//implementation 'org.scala-lang:scala-library:2.12.10'
//
//compile group: 'org.http4s', name: 'http4s-circe_2.12', version: '0.21.0-RC4'
//compile group: 'org.http4s', name: 'http4s-dsl_2.12', version: '0.21.0-RC4'
//compile group: 'org.http4s', name: 'http4s-blaze-server_2.12', version: '0.21.0-RC4'
//compile group: 'org.http4s', name: 'http4s-blaze-client_2.12', version: '0.21.0-RC4'
//
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-core_2.12', version: '0.12.20'
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-json-circe_2.12', version: '0.12.20'
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-http4s-server_2.12', version: '0.12.20'
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-swagger-ui-http4s_2.12', version: '0.12.20'
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-openapi-docs_2.12', version: '0.12.20'
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-openapi-circe-yaml_2.12', version: '0.12.20'
//compile group: 'com.softwaremill.sttp.tapir', name: 'tapir-openapi-circe_2.12', version: '0.12.20'
//
//compile group: 'ch.qos.logback', name: 'logback-classic', version: '1.2.3'
//compile group: 'com.github.pureconfig', name: 'pureconfig_2.12', version: '0.12.2'
//compile group: 'com.beachape', name: 'enumeratum_2.12', version: '1.5.13'
//compile group: 'io.chrisdavenport', name: 'log4cats-slf4j_2.12', version: '1.0.1'
//
//compile group: 'org.tpolecat', name: 'doobie-quill_2.12', version: '0.8.8'
//compile group: 'org.tpolecat', name: 'doobie-h2_2.12', version: '0.8.8'
//compile group: 'org.tpolecat', name: 'doobie-core_2.12', version: '0.8.8'
//compile group: 'org.tpolecat', name: 'doobie-hikari_2.12', version: '0.8.8'
//compile group: 'org.flywaydb', name: 'flyway-core', version: '6.0.1'
//
//compile group: 'org.postgresql', name: 'postgresql', version: '42.2.6'
//compile group: 'mysql', name: 'mysql-connector-java', version: '8.0.17'
//compile group: 'org.mariadb.jdbc', name: 'mariadb-java-client', version: '2.5.4'
//
//testCompile group: 'org.scalatest', name: 'scalatest_2.12', version: '3.0.8'
//testCompile group: 'com.dimafeng', name: 'testcontainers-scala-mysql_2.12', version: '0.35.0'
//testCompile group: 'com.dimafeng', name: 'testcontainers-scala-postgresql_2.12', version: '0.35.0'
//testCompile group: 'com.dimafeng', name: 'testcontainers-scala-mariadb_2.12', version: '0.35.0'
//testCompile group: 'com.dimafeng', name: 'testcontainers-scala_2.12', version: '0.35.0'
//
//
//
//// client
//compile project(':schemakeeper-common')
//compile group: 'com.konghq', name: 'unirest-java', version: '3.1.00'
//
//testCompile group: 'ch.qos.logback', name: 'logback-classic', version: '1.2.3'
//testCompile group: 'org.testcontainers', name: 'testcontainers', version: '1.12.2'
//
//
//
//// avro
//compile project(':schemakeeper-common')
//compile project(':schemakeeper-client')
//
//
//
//// common
//
//
//
//// protobuf
//compile project(':schemakeeper-common')
//compile project(':schemakeeper-client')
//
//compile group: 'org.apache.avro', name: 'avro-protobuf', version: '1.9.0'
//compile group: 'com.google.protobuf', name: 'protobuf-java', version: '3.6.1'
//
//
//
//// thrift
//compile project(':schemakeeper-common')
//compile project(':schemakeeper-client')
//
//compile group: 'org.apache.avro', name: 'avro-thrift', version: '1.9.0'
//compile group: 'org.apache.thrift', name: 'libthrift', version: '0.12.0'
//
//
//
//// kafka-avro
//compile project(':schemakeeper-avro')
//compile project(':schemakeeper-kafka-common')
//
//testImplementation 'org.scala-lang:scala-library:2.12.8'
//testCompile group: 'org.scalatest', name: 'scalatest_2.12', version: '3.0.8'
//testCompile 'io.github.embeddedkafka:embedded-kafka_2.12:2.1.0'
//
//
//
//// kafka-protobuf
//compile project(':schemakeeper-protobuf')
//compile project(':schemakeeper-kafka-common')
//
//testImplementation 'org.scala-lang:scala-library:2.12.8'
//testCompile group: 'org.scalatest', name: 'scalatest_2.12', version: '3.0.8'
//testCompile 'io.github.embeddedkafka:embedded-kafka_2.12:2.1.0'
//testCompile project(':schemakeeper-protobuf').sourceSets.test.output
//
//
//
//// kafka-thrift
//compile project(':schemakeeper-thrift')
//compile project(':schemakeeper-kafka-common')
//
//testImplementation 'org.scala-lang:scala-library:2.12.8'
//testCompile group: 'org.scalatest', name: 'scalatest_2.12', version: '3.0.8'
//testCompile 'io.github.embeddedkafka:embedded-kafka_2.12:2.1.0'
//testCompile project(':schemakeeper-thrift').sourceSets.test.output
//
//
//
//// kafka-common
//compile group: 'org.apache.kafka', name: 'kafka-clients', version: '2.0.0'

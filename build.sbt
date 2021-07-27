import com.typesafe.sbt.packager.docker._

lazy val kindProjectorVersion = "0.13.0"
// avro
lazy val avroVersion = "1.9.0"
lazy val protobufVersion = "3.17.3"
lazy val thriftVersion = "0.12.0"
// server
lazy val http4sVersion = "0.21.0-RC4"
lazy val tapirVersion = "0.12.28"
lazy val pureconfigVersion = "0.16.0"
lazy val enumeratumVersion = "1.7.0"
lazy val log4catsVersion = "1.0.1"
lazy val doobieVersion = "0.13.4"
lazy val flywayVersion = "6.0.1"
lazy val postgresqlDriverVersion = "42.2.6"
lazy val mysqlDriverVersion = "8.0.26"
lazy val mariadbDriverVersion = "2.5.4"
lazy val kafkaClientVersion = "2.1.0"
// client
lazy val unirestVersion = "3.1.04"
// test
lazy val logbackVersion = "1.2.5"
lazy val junitInterface = "0.11"
lazy val munitVersion = "0.7.27"
lazy val testcontainersVersion = "0.39.5"
lazy val testcontainersJavaVersion = "1.15.3"
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
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-feature",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:existentials",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Xlint",
  "-Xlint:constant",
  "-Xlint:inaccessible",
  "-Xlint:nullary-unit",
  "-Xlint:type-parameter-shadow",
  // scala 2.12
  "-Yno-adapted-args",
  "-Xfuture",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:patvars",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard",
  "-Xlint:unsound-match"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions(scalaVersion.value),
  addCompilerPlugin(
    ("org.typelevel" %% "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)
  ),
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.scalameta" %% "munit" % munitVersion % Test,
    ("com.novocode" % "junit-interface" % junitInterface % Test).exclude("junit", "junit-dep")
  ),
  crossPaths := false,
  testFrameworks += new TestFramework("munit.Framework"),
  testOptions += Tests.Argument(TestFrameworks.JUnit),
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
      common,
      server,
      client,
      avro,
      thrift,
      protobuf,
      kafkaCommon,
      kafkaAvro,
      kafkaThrift,
      kafkaProtobuf
    )

lazy val common = project
  .in(file("modules/common"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-common")
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test
    )
  )

lazy val server = project
  .in(file("modules/server"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-server")
  .settings(
    dockerBaseImage := "openjdk:8-jre-alpine",
    dockerRepository := Some("index.docker.io"),
    dockerUsername := Some("nryanov"),
    dockerUpdateLatest := true,
    Docker / packageName := "schemakeeper",
    Docker / version := Option(System.getenv("GIT_TAG_NAME")).getOrElse("test"),
    Docker / daemonUser := "daemon",
    Docker / maintainer := "https://github/nryanov",
    dockerCommands ++= Seq(
      Cmd("USER", "root"),
      Cmd("RUN", "apk", "add", "--no-cache", "bash"),
      Cmd("USER", (Docker / daemonUser).value)
    )
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-blaze-server" % http4sVersion,
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-http4s" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe" % tapirVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4catsVersion,
      "org.tpolecat" %% "doobie-quill" % doobieVersion,
      "org.tpolecat" %% "doobie-h2" % doobieVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.flywaydb" % "flyway-core" % flywayVersion,
      "org.postgresql" % "postgresql" % postgresqlDriverVersion,
      "mysql" % "mysql-connector-java" % mysqlDriverVersion,
      "org.mariadb.jdbc" % "mariadb-java-client" % mariadbDriverVersion,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mariadb" % testcontainersVersion % Test
    )
  )
  .dependsOn(common % compileAndTest)

lazy val client = project
  .in(file("modules/client"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-client")
  .settings(
    libraryDependencies ++= Seq(
      "com.konghq" % "unirest-java" % unirestVersion,
      // todo: replace by mock server ?
      "org.testcontainers" % "testcontainers" % testcontainersJavaVersion % Test
    )
  )
  .dependsOn(common % compileAndTest)

lazy val avro = project
  .in(file("modules/avro"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-avro")
  .dependsOn(common % compileAndTest)
  .dependsOn(client % compileAndTest)

lazy val thrift = project
  .in(file("modules/thrift"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-thrift")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro-thrift" % avroVersion,
      "org.apache.thrift" % "libthrift" % thriftVersion
    )
  )
  .dependsOn(common % compileAndTest)
  .dependsOn(client % compileAndTest)

lazy val protobuf = project
  .in(file("modules/protobuf"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-protobuf")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro-protobuf" % avroVersion,
      "com.google.protobuf" % "protobuf-java" % protobufVersion
    )
  )
  .dependsOn(common % compileAndTest)
  .dependsOn(client % compileAndTest)

lazy val kafkaCommon = project
  .in(file("modules/kafka/common"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-kafka-common")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaClientVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion % Test
    )
  )

lazy val kafkaAvro = project
  .in(file("modules/kafka/avro"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-kafka-avro")
  .dependsOn(kafkaCommon % compileAndTest)
  .dependsOn(avro % compileAndTest)

lazy val kafkaThrift = project
  .in(file("modules/kafka/thrift"))
  .settings(allSettings)
  .settings(moduleName := "schemakeeper-kafka-thrift")
  .dependsOn(kafkaCommon % compileAndTest)
  .dependsOn(thrift % compileAndTest)

lazy val kafkaProtobuf =
  project
    .in(file("modules/kafka/protobuf"))
    .settings(allSettings)
    .settings(moduleName := "schemakeeper-kafka-protobuf")
    .dependsOn(kafkaCommon % compileAndTest)
    .dependsOn(protobuf % compileAndTest)

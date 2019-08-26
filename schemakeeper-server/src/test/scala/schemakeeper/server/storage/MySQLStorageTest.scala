package schemakeeper.server.storage

import java.sql.DriverManager
import java.util

import cats.Id
import com.dimafeng.testcontainers.{ForAllTestContainer, MySQLContainer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, WordSpec}
import schemakeeper.avro.compatibility.CompatibilityType
import schemakeeper.server.Configuration
import schemakeeper.server.service.DBBackedService

import scala.util.Try

class MySQLStorageTest extends WordSpec with ForAllTestContainer with BeforeAndAfterEach with BeforeAndAfterAll {
  override val container: MySQLContainer = MySQLContainer(mysqlImageVersion = "mysql:latest", databaseName = "schemakeeper")

  lazy val schemaStorage = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", container.username)
    map.put("schemakeeper.storage.password", container.password)
    map.put("schemakeeper.storage.driver", container.driverClassName)
    map.put("schemakeeper.storage.url", container.jdbcUrl)

    val config: Config = ConfigFactory.parseMap(map)
    DBBackedService.apply[Id](Configuration.apply(config))
  }

  lazy val connection = {
    Class.forName(container.driverClassName)
    val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
    connection.setAutoCommit(false)
    connection
  }

  override protected def afterEach(): Unit = {
    connection.createStatement().execute("delete from schemakeeper.schema_info")
    connection.createStatement().execute("delete from schemakeeper.subject")
    connection.commit()
  }

  override protected def afterAll(): Unit = {
    connection.close()
  }

  "MySQL backend for schema storage" should {
    "return empty results" when {
      "database is empty" in {
        assert(schemaStorage.subjects().isEmpty)
        assert(schemaStorage.getLastSchema("empty").isEmpty)
        assert(schemaStorage.getLastSchemas("empty").isEmpty)
        assert(schemaStorage.subjectSchemaByVersion("empty", 1).isEmpty)
        assert(schemaStorage.subjectOnlySchemaByVersion("empty", 1).isEmpty)
        assert(schemaStorage.schemaById(1).isEmpty)
      }
    }

    "register new schema and subject" in {
      val schema: Schema = SchemaBuilder
        .builder("test")
        .record("test")
        .fields()
        .requiredString("f1")
        .endRecord()

      val id = schemaStorage.registerNewSubjectSchema("test", schema.toString)

      assert(id > 0)
      assertResult(schemaStorage.getLastSchema("test").get)(schema.toString())
      assertResult(schemaStorage.schemaById(id).get)(schema.toString())
      assertResult(schemaStorage.subjectOnlySchemaByVersion("test", 1).get)(schema.toString())
    }

    "do not register new schema version due to compatibility issues" in {
      val schema: Schema = SchemaBuilder
        .builder("test_namespace")
        .record("test")
        .fields()
        .requiredString("f1")
        .endRecord()

      val updatedSchema: Schema = SchemaBuilder
        .builder("test_namespace")
        .record("test")
        .fields()
        .requiredString("some_name") // replace required field by another field
        .endRecord()

      schemaStorage.registerNewSubjectSchema("test", schema.toString)
      schemaStorage.updateSubjectCompatibility("test", CompatibilityType.Full)
      Try {
        schemaStorage.registerNewSubjectSchema("test", updatedSchema.toString)
      }

      assert(schemaStorage.getLastSchemas("test").length == 1)
    }

    "register multiple schema versions" in {
      val schema: Schema = SchemaBuilder
        .builder("test_namespace")
        .record("test")
        .fields()
        .requiredString("f1")
        .endRecord()

      val updatedSchema: Schema = SchemaBuilder
        .builder("test_namespace")
        .record("test")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()

      schemaStorage.registerNewSubjectSchema("test", schema.toString)
      schemaStorage.updateSubjectCompatibility("test", CompatibilityType.Backward)
      schemaStorage.registerNewSubjectSchema("test", updatedSchema.toString)

      assert(schemaStorage.getLastSchemas("test").length == 2)
      assertResult(List(1, 2))(schemaStorage.subjectVersions("test"))
    }

    "delete specific subject schema version" in {
      schemaStorage.registerNewSubjectSchema("test", Schema.create(Schema.Type.STRING).toString)
      schemaStorage.updateSubjectCompatibility("test", CompatibilityType.None)
      schemaStorage.registerNewSubjectSchema("test", Schema.create(Schema.Type.INT).toString)

      assert(schemaStorage.getLastSchemas("test").length == 2)
      assert(schemaStorage.deleteSubjectVersion("test", 2))
      assert(schemaStorage.getLastSchemas("test").length == 1)
    }

    "delete subject" in {
      schemaStorage.registerNewSubjectSchema("test", Schema.create(Schema.Type.STRING).toString)
      schemaStorage.updateSubjectCompatibility("test", CompatibilityType.None)
      schemaStorage.registerNewSubjectSchema("test", Schema.create(Schema.Type.INT).toString)

      assert(schemaStorage.getLastSchemas("test").length == 2)
      schemaStorage.deleteSubject("test")
      assert(schemaStorage.getLastSchemas("test").isEmpty)
    }
  }
}

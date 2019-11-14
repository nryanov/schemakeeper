package schemakeeper.server.api

import java.sql.DriverManager
import java.util

import cats.effect.IO
import com.twitter.finagle.http.Status
import com.typesafe.config.{Config, ConfigFactory}
import io.finch.Error.{NotPresent, NotValid}
import io.finch._
import io.finch.circe._
import org.apache.avro.Schema
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.api._
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.api.internal.SubjectSettings
import schemakeeper.server.{Configuration, service}
import schemakeeper.server.api.protocol.{ErrorCode, ErrorInfo}
import schemakeeper.server.api.protocol.JsonProtocol._
import schemakeeper.server.service._

import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class SchemaKeeperApiTest extends WordSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  implicit val ctx = IO.contextShift(ExecutionContext.global)

  lazy val schemaStorage: DBBackedService[IO] = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", "")
    map.put("schemakeeper.storage.password", "")
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", "org.h2.Driver")
    map.put("schemakeeper.storage.maxConnections", "1")
    map.put("schemakeeper.storage.url", "jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1")

    val config: Config = ConfigFactory.parseMap(map)
    DBBackedService.apply[IO](Configuration.apply(config))
  }

  lazy val connection = {
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection("jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1", "", "")
    connection.setSchema("schemakeeper")
    connection.setAutoCommit(false)
    connection
  }

  override protected def afterEach(): Unit = {
    connection.createStatement().execute("delete from subject_schema")
    connection.createStatement().execute("delete from schema_info")
    connection.createStatement().execute("delete from subject")
    connection.commit()
  }

  override protected def afterAll(): Unit = {
    connection.close()
  }

  "Subject endpoint" should {
    "return subject list" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjects(Input.get("/v2/subjects")).awaitValueUnsafe()

      assertResult(List("A1"))(result.get)
    }

    "return empty subject list" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjects(Input.get("/v2/subjects")).awaitValueUnsafe()

      assert(result.get.isEmpty)
    }
  }

  "SubjectMetadata endpoint" should {
    "return subject metadata" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectMetadata(Input.get("/v2/subjects/A1")).awaitValueUnsafe()

      assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result.get)
    }

    "NotFound - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectMetadata(Input.get("/v2/subjects/A1")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }
  }

  "UpdateSubjectSettings endpoint" should {
    "return updated subject metadata" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectSettings(CompatibilityType.FORWARD, isLocked = true)
      val result = api.subjectMetadata(Input.put("/v2/subjects/A1").withBody[Application.Json](body)).awaitValueUnsafe()

      assertResult(body)(result.get)
    }

    "NotFound - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectSettings(CompatibilityType.FORWARD, isLocked = true)
      val result = api.subjectMetadata(Input.put("/v2/subjects/A1")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }
  }

  "SubjectVersions endpoint" should {
    "return versions list" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectVersions(Input.get("/v2/subjects/A1/versions")).awaitValueUnsafe()

      assertResult(List(1))(result.get)
    }

    "return empty versions list" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectVersions(Input.get("/v2/subjects/A1/versions")).awaitValueUnsafe()

      assert(result.get.isEmpty)
    }

    "NotFound - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectVersions(Input.get("/v2/subjects/A1/versions")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }
  }

  "SubjectSchemasMetadata endpoint" should {
    "return meta list" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectSchemasMetadata(Input.get("/v2/subjects/A1/schemas")).awaitValueUnsafe()

      assertResult(1)(result.get.length)
    }

    "return empty list" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectSchemasMetadata(Input.get("/v2/subjects/A1/schemas")).awaitValueUnsafe()

      assert(result.get.isEmpty)
    }

    "NotFound - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectSchemasMetadata(Input.get("/v2/subjects/A1/schemas")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }
  }

  "SubjectSchemaByVersion endpoint" should {
    "return meta" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectSchemaByVersion(Input.get("/v2/subjects/A1/versions/1")).awaitValueUnsafe()

      assertResult(Schema.create(Schema.Type.STRING).toString)(result.get.getSchemaText)
    }

    "NotFound - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectSchemaByVersion(Input.get("/v2/subjects/A1/versions/1")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }

    "NotFound - subject has no schema with such version" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.subjectSchemaByVersion(Input.get("/v2/subjects/A1/versions/2")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectSchemaVersionDoesNotExist("A1", 2).msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode), Status.NotFound))(result.get)
    }

    "throws validation error" in {
      val api = SchemaKeeperApi(schemaStorage)
      assertThrows[NotValid](api.subjectSchemaByVersion(Input.get("/v2/subjects/A1/versions/-1")).awaitOutputUnsafe())
    }
  }

  "SchemaById endpoint" should {
    "return meta" in {
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.schemaById(Input.get(s"/v2/schemas/$id")).awaitValueUnsafe()

      assertResult(id)(result.get.getSchemaId)
    }

    "NotFound - schema with specified id does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.schemaById(Input.get(s"/v2/schemas/1")).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIdDoesNotExist(1).msg, ErrorCode.SchemaIdDoesNotExistCode), Status.NotFound))(result.get)
    }

    "throws validation error" in {
      val api = SchemaKeeperApi(schemaStorage)
      assertThrows[NotValid](api.schemaById(Input.get(s"/v2/schemas/-1")).awaitOutputUnsafe())
    }
  }

  "SchemaIdBySubjectAndSchema endpoint" should {
    "return SchemaId" in {
      val id = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync().right.get
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.schemaIdBySubjectAndSchema(Input.post(s"/v2/subjects/A1/schemas/id").withBody[Application.Json](body)).awaitValueUnsafe()

      assertResult(id)(result.get)
    }

    "NotFound - schema is not registered" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.schemaIdBySubjectAndSchema(Input.post(s"/v2/subjects/A1/schemas/id").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIsNotRegistered(Schema.create(Schema.Type.STRING).toString()).msg, ErrorCode.SchemaIsNotRegisteredCode), Status.NotFound))(result.get)
    }

    "BadRequest - schema is not valid" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance("not valid schema")
      val result = api.schemaIdBySubjectAndSchema(Input.post(s"/v2/subjects/A1/schemas/id").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest))(result.get)
    }

    "BadRequest - schema is not connected to subject" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.schemaIdBySubjectAndSchema(Input.post(s"/v2/subjects/A1/schemas/id").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectIsNotConnectedToSchema("A1", id).msg, ErrorCode.SubjectIsNotConnectedToSchemaCode), Status.BadRequest))(result.get)
    }
  }

  "DeleteSubject endpoint" should {
    "return true" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.deleteSubject(Input.delete(s"/v2/subjects/A1")).awaitValueUnsafe()

      assert(result.get)
    }

    "return false" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.deleteSubject(Input.delete(s"/v2/subjects/A1")).awaitValueUnsafe()

      assert(!result.get)
    }
  }

  "DeleteSubjectSchemaByVersion endpoint" should {
    "return true" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.deleteSubjectSchemaByVersion(Input.delete("/v2/subjects/A1/versions/1")).awaitValueUnsafe()

      assert(result.get)
    }

    "BadRequest - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.deleteSubjectSchemaByVersion(Input.delete("/v2/subjects/A1/versions/1")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }

    "BadRequest - specified version does not exist" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.deleteSubjectSchemaByVersion(Input.delete("/v2/subjects/A1/versions/123")).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(service.SubjectSchemaVersionDoesNotExist("A1", 123).msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode), Status.NotFound))(result.get)
    }

    "throws validation error" in {
      val api = SchemaKeeperApi(schemaStorage)
      assertThrows[NotValid](api.deleteSubjectSchemaByVersion(Input.delete("/v2/subjects/A1/versions/-1")).awaitOutputUnsafe())
    }
  }

  "CheckSubjectCompatibility endpoint" should {
    "return true" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.NONE, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance(Schema.create(Schema.Type.INT).toString)
      val result = api.checkSubjectSchemaCompatibility(Input.post("/v2/subjects/A1/compatibility/schemas").withBody[Application.Json](body)).awaitValueUnsafe()
      assert(result.get)
    }

    "return false" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance(Schema.create(Schema.Type.INT).toString)
      val result = api.checkSubjectSchemaCompatibility(Input.post("/v2/subjects/A1/compatibility/schemas").withBody[Application.Json](body)).awaitValueUnsafe()
      assert(!result.get)
    }

    "BadRequest - schema is not a valid avro schema" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance("not valid schema")
      val result = api.checkSubjectSchemaCompatibility(Input.post("/v2/subjects/A1/compatibility/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()

      assertResult(Output.failure(ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest))(result.get)
    }

    "BadRequest - subject does not exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance(Schema.create(Schema.Type.INT).toString)
      val result = api.checkSubjectSchemaCompatibility(Input.post("/v2/subjects/A1/compatibility/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }
  }

  "RegisterSchema endpoint" should {
    "return schemaId" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING).toString)
      val result = api.registerSchema(Input.post("/v2/schemas").withBody[Application.Json](body)).awaitValueUnsafe()
      assert(result.get.isInstanceOf[SchemaId])
    }

    "BadRequest - schema is not valid" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SchemaText.instance("not valid schema")
      val result = api.registerSchema(Input.post("/v2/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest))(result.get)
    }

    "BadRequest - schema is already exist" in {
      val api = SchemaKeeperApi(schemaStorage)
      val schema = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).unsafeRunSync()
      val id = schema.right.get.getSchemaId
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val result = api.registerSchema(Input.post("/v2/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIsAlreadyExist(id, Schema.create(Schema.Type.STRING).toString()).msg, ErrorCode.SchemaIsAlreadyExistCode), Status.BadRequest))(result.get)
    }
  }

  "RegisterSchemaAndSubject endpoint" should {
    "return schemaId" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectAndSchemaRequest.instance(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO, CompatibilityType.BACKWARD)
      val result = api.registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body)).awaitValueUnsafe()
      assert(result.get.isInstanceOf[SchemaId])
    }

    "return schemaId - schema and subject are already registered and connected" in {
      val id = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync().right.get
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectAndSchemaRequest.instance(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO, CompatibilityType.BACKWARD)
      val result = api.registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body)).awaitValueUnsafe()
      assertResult(id)(result.get)
    }

    "BadRequest - schema is not valid" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectAndSchemaRequest.instance("not valid schema", SchemaType.AVRO, CompatibilityType.BACKWARD)
      val result = api.registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest))(result.get)
    }

    "BadRequest - subject is locked" in {
      val api = SchemaKeeperApi(schemaStorage)
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true).unsafeRunSync()
      val body = SubjectAndSchemaRequest.instance(Schema.create(Schema.Type.INT).toString, SchemaType.AVRO, CompatibilityType.BACKWARD)
      val result = api.registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectIsLocked("A1").msg, ErrorCode.SubjectIsLockedErrorCode), Status.BadRequest))(result.get)
    }

    "BadRequest - schema is not compatible" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.BACKWARD, SchemaType.AVRO).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectAndSchemaRequest.instance(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO, CompatibilityType.BACKWARD)
      val result = api.registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(service.SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD).msg, ErrorCode.SchemaIsNotCompatibleCode), Status.BadRequest))(result.get)
    }
  }

  "RegisterSubject endpoint" should {
    "return ok" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)
      val result = api.registerSubject(Input.post("/v2/subjects").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Ok(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)))(result.get)
    }

    "return ok - register locked subject" in {
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true)
      val result = api.registerSubject(Input.post("/v2/subjects").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Ok(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true)))(result.get)
    }

    "BadRequest - subject is already exist" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)
      val result = api.registerSubject(Input.post("/v2/subjects").withBody[Application.Json](body)).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectIsAlreadyExists("A1").msg, ErrorCode.SubjectIsAlreadyExistsCode), Status.BadRequest))(result.get)
    }
  }

  "AddSchemaToSubject endpoint" should {
    "return version number - first schema" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitValueUnsafe()
      assert(result.contains(1))
    }

    "return version number - second schema" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.NONE, SchemaType.AVRO).unsafeRunSync()
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.INT).toString, SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitValueUnsafe()
      assert(result.contains(2))
    }

    "BadRequest - subject and schema are already connected" in {
      val id = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.NONE, SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectIsAlreadyConnectedToSchema("A1", id).msg, ErrorCode.SubjectIsAlreadyConnectedToSchemaCode), Status.BadRequest))(result.get)
    }

    "BadRequest - subject is locked" in {
      val api = SchemaKeeperApi(schemaStorage)
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true).unsafeRunSync()
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectIsLocked("A1").msg, ErrorCode.SubjectIsLockedErrorCode), Status.BadRequest))(result.get)
    }

    "NotFound - subject does not exist" in {
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.INT).toString, SchemaType.AVRO).unsafeRunSync().right.get.getSchemaId
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound))(result.get)
    }

    "NotFound - schema does not exist" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
      val api = SchemaKeeperApi(schemaStorage)
      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/123")).awaitOutputUnsafe()
      assertResult(Output.failure(ErrorInfo(SchemaIdDoesNotExist(123).msg, ErrorCode.SchemaIdDoesNotExistCode), Status.NotFound))(result.get)
    }

    "throws validation error" in {
      val api = SchemaKeeperApi(schemaStorage)
      assertThrows[NotValid](api.addSchemaToSubject(Input.post("/v2/subjects/A1/schemas/-1")).awaitOutputUnsafe())
    }
  }
}

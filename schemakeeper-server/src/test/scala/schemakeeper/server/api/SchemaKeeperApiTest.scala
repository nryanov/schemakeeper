package schemakeeper.server.api

import io.finch.Error.{NotPresent, NotValid}
import io.finch.{Application, Input, NoContent, Ok, Output, Text}
import io.finch.circe._
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}
import schemakeeper.api.{CompatibilityTypeMetadata, SchemaMetadata}
import schemakeeper.schema.CompatibilityType
import schemakeeper.server.service.{InitialDataGenerator, MockService}
import schemakeeper.server.api.protocol.JsonProtocol._

class SchemaKeeperApiTest extends WordSpec with Matchers {
  "Schema by id endpoint" should {
    "return NoContent" when {
      "there is no schema with such id" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result: Option[Output[String]] = api.schema(Input.get("/v1/schema/1")).awaitOutputUnsafe()

        assert(result.isDefined)
        assertResult(result.get)(NoContent[String])
      }
    }

    "return Schema" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result: Option[Output[String]] = api.schema(Input.get("/v1/schema/1")).awaitOutputUnsafe()

      assert(result.isDefined)
      assertResult(result.get)(Ok[String]("S1"))
    }

    "return error" when {
      "schema id is not positive" in {
        val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
        val api = SchemaKeeperApi(service)

        assertThrows[NotValid](api.schema(Input.get("/v1/schema/-1")).awaitOutputUnsafe())
      }
    }
  }

  "Subjects endpoint" should {
    "return empty list" when {
      "there is no registered subjects" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.subjects(Input.get("/v1/subjects")).awaitValueUnsafe()

        assert(result.get.isEmpty)
      }
    }

    "return not empty list" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result = api.subjects(Input.get("/v1/subjects")).awaitValueUnsafe()

      assertResult(result.get)(List("A1"))
    }
  }

  "Subject versions endpoint" should {
    "return NoContent" when {
      "there is no registered subject with such name" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.subjectVersions(Input.get("/v1/subjects/A1/versions")).awaitOutputUnsafe()

        assertResult(result.get)(NoContent[String])
      }
    }

    "return list of subject versions" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"), ("A1", "S2"))))
      val api = SchemaKeeperApi(service)
      val result = api.subjectVersions(Input.get("/v1/subjects/A1/versions")).awaitValueUnsafe()

      assertResult(result.get)(List(1, 2))
    }
  }

  "Subject schema metadata by version endpoint" should {
    "return NoContent" when {
      "there is no registered subject with specified version id" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.subjectSchemaByVersion(Input.get("/v1/subjects/A1/versions/1")).awaitOutputUnsafe()

        assertResult(result.get)(NoContent[SchemaMetadata])
      }
    }

    "return schema by version" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result = api.subjectSchemaByVersion(Input.get("/v1/subjects/A1/versions/1")).awaitValueUnsafe()

      assertResult(result.get)(SchemaMetadata.instance("A1", 1, 1, "S1"))
    }

    "return error" when {
      "schema id is not positive" in {
        val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
        val api = SchemaKeeperApi(service)

        assertThrows[NotValid](api.subjectSchemaByVersion(Input.get("/v1/subjects/A1/versions/-1")).awaitOutputUnsafe())
      }
    }
  }

  "Subject schema by version endpoint" should {
    "return NoContent" when {
      "there is no registered subject with specified version id" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.subjectOnlySchemaByVersion(Input.get("/v1/subjects/A1/versions/1/schema")).awaitOutputUnsafe()

        assertResult(result.get)(NoContent[String])
      }
    }

    "return schema by version" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result = api.subjectOnlySchemaByVersion(Input.get("/v1/subjects/A1/versions/1/schema")).awaitValueUnsafe()

      assertResult(result.get)("S1")
    }

    "return error" when {
      "schema id is not positive" in {
        val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
        val api = SchemaKeeperApi(service)

        assertThrows[NotValid](api.subjectOnlySchemaByVersion(Input.get("/v1/subjects/A1/versions/-1/schema")).awaitOutputUnsafe())
      }
    }
  }

  "Delete subject endpoint" should {
    "return false" when {
      "there is no registered subject with such name" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.deleteSubject(Input.delete("/v1/subjects/A1")).awaitValueUnsafe()

        assert(!result.get)
      }
    }

    "return true" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result = api.deleteSubject(Input.delete("/v1/subjects/A1")).awaitValueUnsafe()

      assert(result.get)
    }
  }

  "Delete subject verion endpoint" should {
    "return false" when {
      "there is no registered subject with such name or version" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.deleteSubjectVersion(Input.delete("/v1/subjects/A1/versions/1")).awaitValueUnsafe()

        assert(!result.get)
      }
    }

    "return true" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result = api.deleteSubjectVersion(Input.delete("/v1/subjects/A1/versions/1")).awaitValueUnsafe()

      assert(result.get)
    }

    "return error" when {
      "version id is not positive" in {
        val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
        val api = SchemaKeeperApi(service)

        assertThrows[NotValid](api.deleteSubjectVersion(Input.delete("/v1/subjects/A1/versions/-1")).awaitOutputUnsafe())
      }
    }
  }

  "Register new subject endpoint" should {
    "return next schema id" in {
      val service = MockService(InitialDataGenerator())
      val api = SchemaKeeperApi(service)
      val result = api.registerNewSubjectSchema(Input.post("/v1/subjects/A1").withBody[Text.Plain]("SCHEMA")).awaitValueUnsafe()

      assertResult(1)(result.get)
    }

    "return error" when {
      "body is empty" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)

        assertThrows[NotPresent](api.registerNewSubjectSchema(Input.post("/v1/subjects/A1").withBody[Text.Plain]("")).awaitOutputUnsafe())
      }
    }
  }

  "Update Subject Compatibility Config endpoint" should {
    "return None" when {
      "there is no registered subjects" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val body = CompatibilityTypeMetadata.instance(CompatibilityType.BACKWARD)
        val result = api.updateSubjectCompatibilityConfig(Input.put("/v1/compatibility/A1").withBody[Application.Json](body)).awaitOutputUnsafe()

        assertResult(result.get)(NoContent[CompatibilityType])
      }
    }

    "return new compatibility type" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val body = CompatibilityTypeMetadata.instance(CompatibilityType.BACKWARD)
      val result = api.updateSubjectCompatibilityConfig(Input.put("/v1/compatibility/A1").withBody[Application.Json](body)).awaitValueUnsafe()

      assertResult(CompatibilityType.BACKWARD)(result.get)
    }
  }

  "Get Subject Compatibility Config endpoint" should {
    "return None" when {
      "there is no registered subjects" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val result = api.getSubjectCompatibilityConfig(Input.get("/v1/compatibility/A1")).awaitOutputUnsafe()

        assertResult(result.get)(NoContent[CompatibilityType])
      }
    }

    "return current compatibility type" in {
      val service = MockService(InitialDataGenerator(Seq(("A1", "S1"))))
      val api = SchemaKeeperApi(service)
      val result = api.getSubjectCompatibilityConfig(Input.get("/v1/compatibility/A1")).awaitValueUnsafe()

      assertResult(CompatibilityType.NONE)(result.get)
    }
  }

  "Check subject compatibility" should {
    "return false" when {
      "there is not registered subjects with specified name" in {
        val service = MockService(InitialDataGenerator())
        val api = SchemaKeeperApi(service)
        val body = Schema.create(Schema.Type.INT).toString
        val result = api.checkSubjectSchemaCompatibility(Input.post("/v1/compatibility/A1").withBody[Text.Plain](body)).awaitValueUnsafe()

        assertResult(false)(result.get)
      }

      "schema is not compatible" in {
        val service = MockService(InitialDataGenerator(Seq(("A1", Schema.create(Schema.Type.STRING).toString))))
        val api = SchemaKeeperApi(service)

        val compatibility = CompatibilityTypeMetadata.instance(CompatibilityType.BACKWARD)
        api.updateSubjectCompatibilityConfig(Input.put("/v1/compatibility/A1").withBody[Application.Json](compatibility)).awaitValueUnsafe()

        val body = Schema.create(Schema.Type.INT).toString
        val result = api.checkSubjectSchemaCompatibility(Input.post("/v1/compatibility/A1").withBody[Text.Plain](body)).awaitValueUnsafe()

        assertResult(false)(result.get)
      }
    }

    "return true" when {
      "schema is compatible" in {
        val service = MockService(InitialDataGenerator(Seq(("A1", Schema.create(Schema.Type.INT).toString))))
        val api = SchemaKeeperApi(service)

        val compatibility = CompatibilityTypeMetadata.instance(CompatibilityType.BACKWARD)
        api.updateSubjectCompatibilityConfig(Input.put("/v1/compatibility/A1").withBody[Application.Json](compatibility)).awaitValueUnsafe()

        val body = Schema.create(Schema.Type.LONG).toString
        val result = api.checkSubjectSchemaCompatibility(Input.post("/v1/compatibility/A1").withBody[Text.Plain](body)).awaitValueUnsafe()

        assertResult(true)(result.get)
      }
    }
  }
}

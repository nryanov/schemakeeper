package schemakeeper.server.service

import org.apache.avro.{Schema, SchemaBuilder}
import org.junit.runner.RunWith
import org.scalatest.EitherValues
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.SchemaKeeperError._
import schemakeeper.server.{IOSpec, service}
import schemakeeper.server.util.Utils

@RunWith(classOf[JUnitRunner])
abstract class ServiceSpec extends IOSpec with EitherValues {
  var schemaStorage: DBBackedService[F]

  "Subjects" should {
    "return empty list" when {
      "storage is empty" in runF {
        for {
          result <- schemaStorage.subjects()
        } yield {
          assert(result.isEmpty)
        }
      }
    }

    "return subject list" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjects()
      } yield {
        assertResult(List("A1"))(result)
      }
    }
  }

  "SubjectMetadata" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectMetadata("A1").attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return subject metadata" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectMetadata("A1")
      } yield {
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result)
      }
    }
  }

  "UpdateSubjectSettings" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = false).attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return updated subject metadata" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = true)
      } yield {
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.FULL, true))(result)
      }
    }
  }

  "SubjectVersions" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectVersions("A1").attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return empty list" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectVersions("A1")
      } yield {
        assert(result.isEmpty)
      }
    }

    "return version list" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.subjectVersions("A1")
      } yield {
        assertResult(List(1))(result)
      }
    }
  }

  "SubjectSchemasMetadata" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectSchemasMetadata("A1").attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return SubjectHasNoRegisteredSchemas" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectSchemasMetadata("A1").attempt
      } yield {
        assertResult(SubjectHasNoRegisteredSchemas("A1"))(result.left.value)
      }
    }

    "return schema metadata list" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.subjectSchemasMetadata("A1")
      } yield {
        val meta = result.head

        assertResult(Schema.create(Schema.Type.STRING).toString)(meta.getSchemaText)
        assertResult(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString))(meta.getSchemaHash)
        assertResult(SchemaType.AVRO)(meta.getSchemaType)
      }
    }
  }

  "SubjectSchemaByVersion" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectSchemaByVersion("A1", 1).attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return SubjectSchemaVersionDoesNotExist" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectSchemaByVersion("A1", 1).attempt
      } yield {
        assertResult(SubjectSchemaVersionDoesNotExist("A1", 1))(result.left.value)
      }
    }

    "return SchemaMetadata" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.subjectSchemaByVersion("A1", 1)
      } yield {
        val meta = result

        assertResult(Schema.create(Schema.Type.STRING).toString)(meta.getSchemaText)
        assertResult(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString))(meta.getSchemaHash)
        assertResult(SchemaType.AVRO)(meta.getSchemaType)
      }
    }
  }

  "SchemaById" should {
    "return SchemaDoesNotExist" in runF {
      for {
        result <- schemaStorage.schemaById(1).attempt
      } yield {
        assertResult(SchemaIdDoesNotExist(1))(result.left.value)
      }
    }

    "return SchemaMetadata" in runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.schemaById(id)
      } yield {
        assertResult(
          SchemaMetadata.instance(
            id,
            Schema.create(Schema.Type.STRING).toString,
            Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString),
            SchemaType.AVRO
          )
        )(result)
      }
    }
  }

  "SchemaIdBySubjectAndSchema" should {
    "return SchemaIsNotValid" in runF {
      for {
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", "not valid schema").attempt
      } yield {
        assertResult(SchemaIsNotValid("not valid schema"))(result.left.value)
      }
    }

    "return SchemaIsNotRegistered" in runF {
      for {
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString()).attempt
      } yield {
        assertResult(SchemaIsNotRegistered(Schema.create(Schema.Type.STRING).toString()))(result.left.value)
      }
    }

    "return SubjectIsNotConnectedToSchema" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString()).attempt
      } yield {
        assertResult(SubjectIsNotConnectedToSchema("A1", id))(result.left.value)
      }
    }

    "return SchemaId" in runF {
      for {
        schemaId <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString(),
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      } yield {
        assertResult(schemaId)(result)
      }
    }
  }

  "DeleteSubject" should {
    "return false" when {
      "subject does not exist" in runF {
        for {
          result <- schemaStorage.deleteSubject("A1")
        } yield {
          assertResult(false)(result)
        }
      }
    }

    "return true" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.deleteSubject("A1")
      } yield {
        assertResult(true)(result)
      }
    }
  }

  "DeleteSubjectSchemaByVersion" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1).attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return SubjectSchemaVersionDoesNotExist" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1).attempt
      } yield {
        assertResult(SubjectSchemaVersionDoesNotExist("A1", 1))(result.left.value)
      }
    }

    "return true" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      } yield {
        assert(result)
      }
    }
  }

  "CheckSubjectCompatibility" should {
    "return SchemaIsNotValid" in runF {
      for {
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", "SCHEMA").attempt
      } yield {
        assertResult(SchemaIsNotValid("SCHEMA"))(result.left.value)
      }
    }

    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage
          .checkSubjectSchemaCompatibility("A1", Schema.create(Schema.Type.STRING).toString())
          .attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return true" in runF {

      val schema1 = SchemaBuilder.builder().record("test").fields().requiredString("f1").endRecord()
      val schema2 =
        SchemaBuilder.builder().record("test").fields().requiredString("f1").optionalString("f2").endRecord()

      for {
        _ <- schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.BACKWARD, SchemaType.AVRO)
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())
      } yield {
        assert(result)
      }
    }

    "return false" in runF {
      val schema1 = SchemaBuilder.builder().record("test").fields().requiredString("f1").endRecord()
      val schema2 = SchemaBuilder.builder().record("test").fields().optionalString("f2").endRecord()

      for {
        _ <- schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.FORWARD, SchemaType.AVRO)
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())
      } yield {
        assert(!result)
      }
    }
  }

  "GetSubjectSchemas" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.getSubjectSchemas("A1").attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return SubjectHasNoRegisteredSchemas" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.getSubjectSchemas("A1").attempt
      } yield {
        assertResult(SubjectHasNoRegisteredSchemas("A1"))(result.left.value)
      }
    }

    "return last schema" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString(),
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.getSubjectSchemas("A1")
      } yield {
        assert(result.size == 1)
      }
    }
  }

  "RegisterSchema (only)" should {
    "do not register new schema due to schema is not a valid avro schema" in runF {
      for {
        result <- schemaStorage.registerSchema("SCHEMA", SchemaType.AVRO).attempt
      } yield {
        assertResult(SchemaIsNotValid("SCHEMA"))(result.left.value)
      }
    }

    "register new schema and return schemaId" in runF {
      for {
        result <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).attempt
      } yield {
        assert(result.isRight)
      }
    }

    "does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema" in runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).attempt
      } yield {
        assertResult(SchemaIsAlreadyExist(id, Schema.create(Schema.Type.STRING).toString))(result.left.value)
      }
    }
  }

  "RegisterSchema and subject if does not exist and connect to each other" should {
    "return SubjectIsLocked" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        result <- schemaStorage
          .registerSchema(
            "A1",
            Schema.create(Schema.Type.STRING).toString,
            CompatibilityType.BACKWARD,
            SchemaType.AVRO
          )
          .attempt
      } yield {
        assertResult(SubjectIsLocked("A1"))(result.left.value)
      }
    }

    "do not register new schema and subject due to schema is not a valid avro schema" in runF {
      for {
        result <- schemaStorage.registerSchema("A1", "SCHEMA", CompatibilityType.BACKWARD, SchemaType.AVRO).attempt
        subjects <- schemaStorage.subjects()
      } yield {
        assertResult(SchemaIsNotValid("SCHEMA"))(result.left.value)
        assert(subjects.isEmpty)
      }
    }

    "register subject, register new schema and return schemaId" in runF {
      for {
        result <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        subjects <- schemaStorage.subjects()
        a1 <- schemaStorage.getSubjectSchemas("A1")
      } yield {
        assert(subjects.size == 1)
        assertResult(result.getSchemaId)(a1.head.getSchemaId)
      }
    }

    "register subject, does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema" in runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        subjects <- schemaStorage.subjects()
      } yield {
        assert(subjects.size == 1)
        assertResult(result.getSchemaId)(id)
      }
    }

    "does not register schema and subject and does not connect schema and subject because they are already connected" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        id <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        _ <- schemaStorage.addSchemaToSubject("A1", id.getSchemaId)
        result <- schemaStorage
          .registerSchema(
            "A1",
            Schema.create(Schema.Type.STRING).toString,
            CompatibilityType.BACKWARD,
            SchemaType.AVRO
          )
          .attempt
      } yield {
        assertResult(SubjectIsAlreadyConnectedToSchema("A1", id.getSchemaId))(result.left.value)
      }
    }

    "does not connect schema and subject because new schema is not compatible" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        // CompatibilityType should not be changed from BACKWARD to NONE
        result <- schemaStorage
          .registerSchema(
            "A1",
            Schema.create(Schema.Type.INT).toString,
            CompatibilityType.NONE,
            SchemaType.AVRO
          )
          .attempt
      } yield {
        assertResult(SchemaIsNotCompatible("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.BACKWARD))(
          result.left.value
        )
      }
    }
  }

  "RegisterSubject" should {
    "register new subject" in runF {
      for {
        before <- schemaStorage.subjects()
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        after <- schemaStorage.subjects()
      } yield {
        assert(before.isEmpty)
        assertResult(after)(List("A1"))
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result)
      }
    }

    "register new locked subject" in runF {
      for {
        before <- schemaStorage.subjects()
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        after <- schemaStorage.subjects()
      } yield {
        assert(before.isEmpty)
        assertResult(after)(List("A1"))
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true))(result)
      }
    }

    "return SubjectIsAlreadyExists" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).attempt
      } yield {
        assertResult(SubjectIsAlreadyExists("A1"))(result.left.value)
      }
    }
  }

  "AddSchemaToSubject" should {
    "successfully add schema to subject" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id)
      } yield {
        // 1 - version
        assertResult(1)(result)
      }
    }

    "return SchemaIsNotCompatible" in runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString(),
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assertResult(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.BACKWARD)
        )(result.left.value)
      }
    }

    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.addSchemaToSubject("A1", 1).attempt
      } yield {
        assertResult(SubjectDoesNotExist("A1"))(result.left.value)
      }
    }

    "return SubjectIsAlreadyConnectedToSchema" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        _ <- schemaStorage.addSchemaToSubject("A1", id)
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assertResult(SubjectIsAlreadyConnectedToSchema("A1", id))(result.left.value)
      }
    }

    "return SchemaDoesNotExist" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.addSchemaToSubject("A1", 1).attempt
      } yield {
        assertResult(SchemaIdDoesNotExist(1))(result.left.value)
      }
    }

    "return SubjectIsLocked" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        result <- schemaStorage.addSchemaToSubject("A1", 1).attempt
      } yield {
        assertResult(SubjectIsLocked("A1"))(result.left.value)
      }
    }
  }
}

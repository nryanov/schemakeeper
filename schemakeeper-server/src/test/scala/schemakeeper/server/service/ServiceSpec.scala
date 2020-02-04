package schemakeeper.server.service

import org.apache.avro.{Schema, SchemaBuilder}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.{IOSpec, service}
import schemakeeper.server.util.Utils

@RunWith(classOf[JUnitRunner])
abstract class ServiceSpec extends IOSpec {
  var schemaStorage: DBBackedService[F]

  "Subjects" should {
    "return empty list" when {
      "storage is empty" in runF {
        for {
          result <- schemaStorage.subjects()
        } yield {
          assert(result.isRight)
          assert(result.right.get.isEmpty)
        }
      }
    }

    "return subject list" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjects()
      } yield {
        assert(result.isRight)
        assertResult(List("A1"))(result.right.get)
      }
    }
  }

  "SubjectMetadata" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectMetadata("A1")
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return subject metadata" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectMetadata("A1")
      } yield {
        assert(result.isRight)
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result.right.get)
      }
    }
  }

  "UpdateSubjectSettings" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = false)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return updated subject metadata" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = true)
      } yield {
        assert(result.isRight)
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.FULL, true))(result.right.get)
      }
    }
  }

  "SubjectVersions" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectVersions("A1")
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return empty list" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectVersions("A1")
      } yield {
        assert(result.isRight)
        assert(result.right.get.isEmpty)
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
        assert(result.isRight)
        assertResult(List(1))(result.right.get)
      }
    }
  }

  "SubjectSchemasMetadata" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectSchemasMetadata("A1")
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return SubjectHasNoRegisteredSchemas" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectSchemasMetadata("A1")
      } yield {
        assert(result.isRight)
        assert(result.right.get.isEmpty)
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
        assert(result.isRight)

        val meta = result.right.get.head

        assertResult(Schema.create(Schema.Type.STRING).toString)(meta.getSchemaText)
        assertResult(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString))(meta.getSchemaHash)
        assertResult(SchemaType.AVRO)(meta.getSchemaType)
      }
    }
  }

  "SubjectSchemaByVersion" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.subjectSchemaByVersion("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return SubjectSchemaVersionDoesNotExist" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectSchemaByVersion("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectSchemaVersionDoesNotExist("A1", 1))(result.left.get)
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
        assert(result.isRight)

        val meta = result.right.get

        assertResult(Schema.create(Schema.Type.STRING).toString)(meta.getSchemaText)
        assertResult(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString))(meta.getSchemaHash)
        assertResult(SchemaType.AVRO)(meta.getSchemaType)
      }
    }
  }

  "SchemaById" should {
    "return SchemaDoesNotExist" in runF {
      for {
        result <- schemaStorage.schemaById(1)
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIdDoesNotExist(1))(result.left.get)
      }
    }

    "return SchemaMetadata" in runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.right.get.getSchemaId
        result <- schemaStorage.schemaById(id)
      } yield {
        assert(result.isRight)
        assertResult(
          SchemaMetadata.instance(
            id,
            Schema.create(Schema.Type.STRING).toString,
            Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString),
            SchemaType.AVRO
          )
        )(result.right.get)
      }
    }
  }

  "SchemaIdBySubjectAndSchema" should {
    "return SchemaIsNotValid" in runF {
      for {
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", "not valid schema")
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsNotValid("not valid schema"))(result.left.get)
      }
    }

    "return SchemaIsNotRegistered" in runF {
      for {
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsNotRegistered(Schema.create(Schema.Type.STRING).toString()))(result.left.get)
      }
    }

    "return SubjectIsNotConnectedToSchema" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.right.get.getSchemaId
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      } yield {
        assert(result.isLeft)
        assertResult(SubjectIsNotConnectedToSchema("A1", id))(result.left.get)
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
        id = schemaId.right.get.getSchemaId
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      } yield {
        assert(result.isRight)
        assertResult(schemaId.right.get)(result.right.get)
      }
    }
  }

  "DeleteSubject" should {
    "return false" when {
      "subject does not exist" in runF {
        for {
          result <- schemaStorage.deleteSubject("A1")
        } yield {
          assert(result.isRight)
          assertResult(false)(result.right.get)
        }
      }
    }

    "return true" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.deleteSubject("A1")
      } yield {
        assert(result.isRight)
        assertResult(true)(result.right.get)
      }
    }
  }

  "DeleteSubjectSchemaByVersion" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return SubjectSchemaVersionDoesNotExist" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(service.SubjectSchemaVersionDoesNotExist("A1", 1))(result.left.get)
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
        assert(result.isRight)
        assertResult(true)(result.right.get)
      }
    }
  }

  "CheckSubjectCompatibility" should {
    "return SchemaIsNotValid" in runF {
      for {
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", "SCHEMA")
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsNotValid("SCHEMA"))(result.left.get)
      }
    }

    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", Schema.create(Schema.Type.STRING).toString())
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
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
        assert(result.isRight)
        assertResult(true)(result.right.get)
      }
    }

    "return false" in runF {
      val schema1 = SchemaBuilder.builder().record("test").fields().requiredString("f1").endRecord()
      val schema2 = SchemaBuilder.builder().record("test").fields().optionalString("f2").endRecord()

      for {
        _ <- schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.FORWARD, SchemaType.AVRO)
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())
      } yield {
        assert(result.isRight)
        assertResult(false)(result.right.get)
      }
    }
  }

  "GetSubjectSchemas" should {
    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.getSubjectSchemas("A1")
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return SubjectHasNoRegisteredSchemas" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.getSubjectSchemas("A1")
      } yield {
        assert(result.isLeft)
        assertResult(SubjectHasNoRegisteredSchemas("A1"))(result.left.get)
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
        assert(result.isRight)
        assert(result.right.get.size == 1)
      }
    }
  }

  "RegisterSchema (only)" should {
    "do not register new schema due to schema is not a valid avro schema" in runF {
      for {
        result <- schemaStorage.registerSchema("SCHEMA", SchemaType.AVRO)
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsNotValid("SCHEMA"))(result.left.get)
      }
    }

    "register new schema and return schemaId" in runF {
      for {
        result <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
      } yield {
        assert(result.isRight)
      }
    }

    "does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema" in runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.right.get.getSchemaId
        result <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsAlreadyExist(id, Schema.create(Schema.Type.STRING).toString))(result.left.get)
      }
    }
  }

  "RegisterSchema and subject if does not exist and connect to each other" should {
    "return SubjectIsLocked" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        result <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
      } yield {
        assert(result.isLeft)
        assertResult(SubjectIsLocked("A1"))(result.left.get)
      }
    }

    "do not register new schema and subject due to schema is not a valid avro schema" in runF {
      for {
        result <- schemaStorage.registerSchema("A1", "SCHEMA", CompatibilityType.BACKWARD, SchemaType.AVRO)
        subjects <- schemaStorage.subjects()
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsNotValid("SCHEMA"))(result.left.get)
        assert(subjects.right.get.isEmpty)
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
        assert(result.isRight)
        assert(subjects.right.get.size == 1)
        assertResult(result.right.get.getSchemaId)(a1.right.get.head.getSchemaId)
      }
    }

    "register subject, does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema" in runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.right.get.getSchemaId
        result <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        subjects <- schemaStorage.subjects()
      } yield {
        assert(result.isRight)
        assert(subjects.right.get.size == 1)
        assertResult(result.right.get.getSchemaId)(id)
      }
    }

    "does not register schema and subject and does not connect schema and subject because they are already connected" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        id <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        _ <- schemaStorage.addSchemaToSubject("A1", id.right.get.getSchemaId)
        result <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
      } yield {
        assert(result.isLeft)
        assertResult(SubjectIsAlreadyConnectedToSchema("A1", id.right.get.getSchemaId))(result.left.get)
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
        result <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString,
          CompatibilityType.NONE,
          SchemaType.AVRO
        )
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIsNotCompatible("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.BACKWARD))(
          result.left.get
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
        assert(before.right.get.isEmpty)
        assertResult(after.right.get)(List("A1"))
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result.right.get)
      }
    }

    "register new locked subject" in runF {
      for {
        before <- schemaStorage.subjects()
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        after <- schemaStorage.subjects()
      } yield {
        assert(before.right.get.isEmpty)
        assertResult(after.right.get)(List("A1"))
        assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true))(result.right.get)
      }
    }

    "return SubjectIsAlreadyExists" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectIsAlreadyExists("A1"))(result.left.get)
      }
    }
  }

  "AddSchemaToSubject" should {
    "successfully add schema to subject" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.right.get.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id)
      } yield {
        assert(result.isRight)
        // 1 - version
        assertResult(1)(result.right.get)
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
        id = schema.right.get.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id)
      } yield {
        assert(result.isLeft)
        assertResult(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.BACKWARD)
        )(result.left.get)
      }
    }

    "return SubjectDoesNotExist" in runF {
      for {
        result <- schemaStorage.addSchemaToSubject("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectDoesNotExist("A1"))(result.left.get)
      }
    }

    "return SubjectIsAlreadyConnectedToSchema" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.right.get.getSchemaId
        _ <- schemaStorage.addSchemaToSubject("A1", id)
        result <- schemaStorage.addSchemaToSubject("A1", id)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectIsAlreadyConnectedToSchema("A1", id))(result.left.get)
      }
    }

    "return SchemaDoesNotExist" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.addSchemaToSubject("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(SchemaIdDoesNotExist(1))(result.left.get)
      }
    }

    "return SubjectIsLocked" in runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        result <- schemaStorage.addSchemaToSubject("A1", 1)
      } yield {
        assert(result.isLeft)
        assertResult(SubjectIsLocked("A1"))(result.left.get)
      }
    }
  }
}

package schemakeeper.server.service

import org.apache.avro.{Schema, SchemaBuilder}
import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.SchemaKeeperError._
import schemakeeper.server.IOSpec
import schemakeeper.server.util.Utils

abstract class ServiceSpec extends IOSpec {
  var schemaStorage: DBBackedService[F]

  test("Subjects should return empty list when storage is empty") {
    runF {
      for {
        result <- schemaStorage.subjects()
      } yield assert(result.isEmpty)
    }
  }

  test("Subjects should return subject list") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjects()
      } yield assertEquals(List("A1"), result)
    }
  }

  test("SubjectMetadata should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.subjectMetadata("A1").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("SubjectMetadata should return subject metadata") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectMetadata("A1")
      } yield assertEquals(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD), result)
    }
  }

  test("UpdateSubjectSettings should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = false).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("UpdateSubjectSettings should return updated subject metadata") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = true)
      } yield assertEquals(SubjectMetadata.instance("A1", CompatibilityType.FULL, true), result)
    }
  }

  test("SubjectVersions should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.subjectVersions("A1").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("SubjectVersions should return empty list") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectVersions("A1")
      } yield assert(result.isEmpty)
    }
  }

  test("SubjectVersions should return version list") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.subjectVersions("A1")
      } yield assertEquals(List(1), result)
    }
  }

  test("SubjectSchemasMetadata should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.subjectSchemasMetadata("A1").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("SubjectSchemasMetadata should return SubjectHasNoRegisteredSchemas") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectSchemasMetadata("A1").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectHasNoRegisteredSchemas("A1"), result.left.get.asInstanceOf[SubjectHasNoRegisteredSchemas])
      }
    }
  }

  test("SubjectSchemasMetadata should return schema metadata list") {
    runF {
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

        assertEquals(Schema.create(Schema.Type.STRING).toString, meta.getSchemaText)
        assertEquals(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString), meta.getSchemaHash)
        assertEquals(SchemaType.AVRO, meta.getSchemaType)
      }
    }
  }

  test("SubjectSchemaByVersion should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.subjectSchemaByVersion("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("SubjectSchemaByVersion should return SubjectSchemaVersionDoesNotExist") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.subjectSchemaByVersion("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SubjectSchemaVersionDoesNotExist("A1", 1),
          result.left.get.asInstanceOf[SubjectSchemaVersionDoesNotExist]
        )
      }
    }
  }

  test("SubjectSchemaByVersion should return SchemaMetadata") {
    runF {
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

        assertEquals(Schema.create(Schema.Type.STRING).toString, meta.getSchemaText)
        assertEquals(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString), meta.getSchemaHash)
        assertEquals(SchemaType.AVRO, meta.getSchemaType)
      }
    }
  }

  test("SchemaById should return SchemaDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.schemaById(1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SchemaIdDoesNotExist(1), result.left.get.asInstanceOf[SchemaIdDoesNotExist])
      }
    }
  }

  test("SchemaById should return SchemaMetadata") {
    runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.schemaById(id)
      } yield assertEquals(
        SchemaMetadata.instance(
          id,
          Schema.create(Schema.Type.STRING).toString,
          Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString),
          SchemaType.AVRO
        ),
        result
      )
    }
  }

  test("SchemaIdBySubjectAndSchema should return SchemaIsNotValid") {
    runF {
      for {
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", "not valid schema").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SchemaIsNotValid("not valid schema"), result.left.get.asInstanceOf[SchemaIsNotValid])
      }
    }
  }

  test("SchemaIdBySubjectAndSchema should return SchemaIsNotRegistered") {
    runF {
      for {
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString()).attempt
      } yield assertEquals(
        SchemaIsNotRegistered(Schema.create(Schema.Type.STRING).toString()),
        result.left.get.asInstanceOf[SchemaIsNotRegistered]
      )
    }
  }

  test("SchemaIdBySubjectAndSchema should return SubjectIsNotConnectedToSchema") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString()).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SubjectIsNotConnectedToSchema("A1", id),
          result.left.get.asInstanceOf[SubjectIsNotConnectedToSchema]
        )
      }
    }
  }

  test("SchemaIdBySubjectAndSchema should return SchemaId") {
    runF {
      for {
        schemaId <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString(),
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      } yield assertEquals(schemaId, result)
    }
  }

  test("DeleteSubject should return false when subject does not exist") {
    runF {
      for {
        result <- schemaStorage.deleteSubject("A1")
      } yield assertEquals(false, result)
    }
  }

  test("DeleteSubject should return return true") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.deleteSubject("A1")
      } yield assertEquals(true, result)
    }
  }

  test("DeleteSubjectSchemaByVersion should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("DeleteSubjectSchemaByVersion should return SubjectSchemaVersionDoesNotExist") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SubjectSchemaVersionDoesNotExist("A1", 1),
          result.left.get.asInstanceOf[SubjectSchemaVersionDoesNotExist]
        )
      }
    }
  }

  test("DeleteSubjectSchemaByVersion should return true") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      } yield assert(result)
    }
  }

  test("CheckSubjectCompatibility should return SchemaIsNotValid") {
    runF {
      for {
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", "SCHEMA").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SchemaIsNotValid("SCHEMA"), result.left.get.asInstanceOf[SchemaIsNotValid])
      }
    }
  }

  test("CheckSubjectCompatibility should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage
          .checkSubjectSchemaCompatibility("A1", Schema.create(Schema.Type.STRING).toString())
          .attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("CheckSubjectCompatibility should return true") {
    runF {

      val schema1 = SchemaBuilder.builder().record("test").fields().requiredString("f1").endRecord()
      val schema2 =
        SchemaBuilder.builder().record("test").fields().requiredString("f1").optionalString("f2").endRecord()

      for {
        _ <- schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.BACKWARD, SchemaType.AVRO)
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())
      } yield assert(result)
    }
  }

  test("CheckSubjectCompatibility should return false") {
    runF {
      val schema1 = SchemaBuilder.builder().record("test").fields().requiredString("f1").endRecord()
      val schema2 = SchemaBuilder.builder().record("test").fields().optionalString("f2").endRecord()

      for {
        _ <- schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.FORWARD, SchemaType.AVRO)
        result <- schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())
      } yield assert(!result)
    }
  }

  test("GetSubjectSchemas should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.getSubjectSchemas("A1").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("GetSubjectSchemas should return SubjectHasNoRegisteredSchemas") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.getSubjectSchemas("A1").attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectHasNoRegisteredSchemas("A1"), result.left.get.asInstanceOf[SubjectHasNoRegisteredSchemas])
      }
    }
  }

  test("GetSubjectSchemas should return last schema") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString(),
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        result <- schemaStorage.getSubjectSchemas("A1")
      } yield assert(result.size == 1)
    }
  }

  test("RegisterSchema (only) should do not register new schema due to schema is not a valid avro schema") {
    runF {
      for {
        result <- schemaStorage.registerSchema("SCHEMA", SchemaType.AVRO).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SchemaIsNotValid("SCHEMA"), result.left.get.asInstanceOf[SchemaIsNotValid])
      }
    }
  }

  test("RegisterSchema (only) should register new schema and return schemaId") {
    runF {
      for {
        result <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).attempt
      } yield assert(result.isRight)
    }
  }

  test(
    "RegisterSchema (only) should does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema"
  ) {
    runF {
      for {
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).attempt
      } yield assertEquals(
        SchemaIsAlreadyExist(id, Schema.create(Schema.Type.STRING).toString),
        result.left.get.asInstanceOf[SchemaIsAlreadyExist]
      )
    }
  }

  test("RegisterSchema and subject if does not exist and connect to each other should return SubjectIsLocked") {
    runF {
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
        assert(result.isLeft)
        assertEquals(SubjectIsLocked("A1"), result.left.get.asInstanceOf[SubjectIsLocked])
      }
    }
  }

  test(
    "RegisterSchema and subject if does not exist and connect to each other should do not register new schema and subject due to schema is not a valid avro schema"
  ) {
    runF {
      for {
        result <- schemaStorage.registerSchema("A1", "SCHEMA", CompatibilityType.BACKWARD, SchemaType.AVRO).attempt
        subjects <- schemaStorage.subjects()
      } yield {
        assert(result.isLeft)
        assertEquals(SchemaIsNotValid("SCHEMA"), result.left.get.asInstanceOf[SchemaIsNotValid])
        assert(subjects.isEmpty)
      }
    }
  }

  test("RegisterSchema and subject should register subject, register new schema and return schemaId") {
    runF {
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
        assertEquals(result.getSchemaId, a1.head.getSchemaId)
      }
    }
  }

  test(
    "RegisterSchema and subject if does not exist and connect to each other should register subject, does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema"
  ) {
    runF {
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
        assertEquals(result.getSchemaId, id)
      }
    }
  }

  test(
    "RegisterSchema and subject if does not exist and connect to each other should does not register schema and subject and does not connect schema and subject because they are already connected"
  ) {
    runF {
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
        assert(result.isLeft)
        assertEquals(
          SubjectIsAlreadyConnectedToSchema("A1", id.getSchemaId),
          result.left.get.asInstanceOf[SubjectIsAlreadyConnectedToSchema]
        )
      }
    }
  }

  test(
    "RegisterSchema and subject if does not exist and connect to each other should does not connect schema and subject because new schema is not compatible"
  ) {
    runF {
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
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.BACKWARD),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("RegisterSubject should register new subject") {
    runF {
      for {
        before <- schemaStorage.subjects()
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        after <- schemaStorage.subjects()
      } yield {
        assert(before.isEmpty)
        assertEquals(after, List("A1"))
        assertEquals(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD), result)
      }
    }
  }

  test("RegisterSubject should register new locked subject") {
    runF {
      for {
        before <- schemaStorage.subjects()
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        after <- schemaStorage.subjects()
      } yield {
        assert(before.isEmpty)
        assertEquals(after, List("A1"))
        assertEquals(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true), result)
      }
    }
  }

  test("RegisterSubject should return SubjectIsAlreadyExists") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectIsAlreadyExists("A1"), result.left.get.asInstanceOf[SubjectIsAlreadyExists])
      }
    }
  }

  test("AddSchemaToSubject should successfully add schema to subject") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id)
      } yield
      // 1 - version
      assertEquals(1, result)
    }
  }

  test("AddSchemaToSubject should return SchemaIsNotCompatible (primitive types) - backward") {
    runF {
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
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.BACKWARD),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SchemaIsNotCompatible (primitive types) - forward") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString(),
          CompatibilityType.FORWARD,
          SchemaType.AVRO
        )
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.FORWARD),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SchemaIsNotCompatible (primitive types) - full") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString(),
          CompatibilityType.FULL,
          SchemaType.AVRO
        )
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.FULL),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SchemaIsNotCompatible (primitive types) - backward transitive") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString(),
          CompatibilityType.BACKWARD_TRANSITIVE,
          SchemaType.AVRO
        )
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible(
            "A1",
            Schema.create(Schema.Type.STRING).toString(),
            CompatibilityType.BACKWARD_TRANSITIVE
          ),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SchemaIsNotCompatible (primitive types) - forward transitive") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString(),
          CompatibilityType.FORWARD_TRANSITIVE,
          SchemaType.AVRO
        )
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible(
            "A1",
            Schema.create(Schema.Type.STRING).toString(),
            CompatibilityType.FORWARD_TRANSITIVE
          ),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SchemaIsNotCompatible (primitive types) - full transitive") {
    runF {
      for {
        _ <- schemaStorage.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString(),
          CompatibilityType.FULL_TRANSITIVE,
          SchemaType.AVRO
        )
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.FULL_TRANSITIVE),
          result.left.get.asInstanceOf[SchemaIsNotCompatible]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SubjectDoesNotExist") {
    runF {
      for {
        result <- schemaStorage.addSchemaToSubject("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectDoesNotExist("A1"), result.left.get.asInstanceOf[SubjectDoesNotExist])
      }
    }
  }

  test("AddSchemaToSubject should return SubjectIsAlreadyConnectedToSchema") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        schema <- schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        id = schema.getSchemaId
        _ <- schemaStorage.addSchemaToSubject("A1", id)
        result <- schemaStorage.addSchemaToSubject("A1", id).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(
          SubjectIsAlreadyConnectedToSchema("A1", id),
          result.left.get.asInstanceOf[SubjectIsAlreadyConnectedToSchema]
        )
      }
    }
  }

  test("AddSchemaToSubject should return SchemaDoesNotExist") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        result <- schemaStorage.addSchemaToSubject("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SchemaIdDoesNotExist(1), result.left.get.asInstanceOf[SchemaIdDoesNotExist])
      }
    }
  }

  test("AddSchemaToSubject should return SubjectIsLocked") {
    runF {
      for {
        _ <- schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        result <- schemaStorage.addSchemaToSubject("A1", 1).attempt
      } yield {
        assert(result.isLeft)
        assertEquals(SubjectIsLocked("A1"), result.left.get.asInstanceOf[SubjectIsLocked])
      }
    }
  }
}

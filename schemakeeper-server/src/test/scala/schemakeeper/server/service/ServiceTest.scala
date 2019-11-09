package schemakeeper.server.service

import cats.Id
import org.apache.avro.{Schema, SchemaBuilder}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.service
import schemakeeper.server.util.Utils

@RunWith(classOf[JUnitRunner])
abstract class ServiceTest extends WordSpec with Matchers {
  val schemaStorage: DBBackedService[Id]

  "Subjects" should {
    "return empty list" when {
      "storage is empty" in {
        val result = schemaStorage.subjects()
        assert(result.isRight)
        assert(result.right.get.isEmpty)
      }
    }

    "return subject list" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.subjects()
      assert(result.isRight)
      assertResult(List("A1"))(result.right.get)
    }
  }

  "SubjectMetadata" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.subjectMetadata("A1")
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return subject metadata" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.subjectMetadata("A1")
      assert(result.isRight)
      assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result.right.get)
    }
  }

  "UpdateSubjectSettings" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = false)
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return updated subject metadata" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.updateSubjectSettings("A1", CompatibilityType.FULL, isLocked = true)
      assert(result.isRight)
      assertResult(SubjectMetadata.instance("A1", CompatibilityType.FULL, true))(result.right.get)
    }
  }

  "SubjectVersions" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.subjectVersions("A1")
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return empty list" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.subjectVersions("A1")
      assert(result.isRight)
      assert(result.right.get.isEmpty)
    }

    "return version list" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      val result = schemaStorage.subjectVersions("A1")
      assert(result.isRight)
      assertResult(List(1))(result.right.get)
    }
  }

  "SubjectSchemasMetadata" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.subjectSchemasMetadata("A1")
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return SubjectHasNoRegisteredSchemas" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.subjectSchemasMetadata("A1")
      assert(result.isRight)
      assert(result.right.get.isEmpty)
    }

    "return schema metadata list" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      val result = schemaStorage.subjectSchemasMetadata("A1")
      assert(result.isRight)

      val meta = result.right.get.head

      assertResult(Schema.create(Schema.Type.STRING).toString)(meta.getSchemaText)
      assertResult(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString))(meta.getSchemaHash)
      assertResult(SchemaType.AVRO)(meta.getSchemaType)

    }
  }

  "SubjectSchemaByVersion" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.subjectSchemaByVersion("A1", 1)
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return SubjectSchemaVersionDoesNotExist" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.subjectSchemaByVersion("A1", 1)
      assert(result.isLeft)
      assertResult(SubjectSchemaVersionDoesNotExist("A1", 1))(result.left.get)
    }

    "return SchemaMetadata" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      val result = schemaStorage.subjectSchemaByVersion("A1", 1)
      assert(result.isRight)

      val meta = result.right.get

      assertResult(Schema.create(Schema.Type.STRING).toString)(meta.getSchemaText)
      assertResult(Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString))(meta.getSchemaHash)
      assertResult(SchemaType.AVRO)(meta.getSchemaType)
    }
  }

  "SchemaById" should {
    "return SchemaDoesNotExist" in {
      val result = schemaStorage.schemaById(1)
      assert(result.isLeft)
      assertResult(SchemaIdDoesNotExist(1))(result.left.get)
    }

    "return SchemaMetadata" in {
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).right.get.getSchemaId
      val result = schemaStorage.schemaById(id)
      assert(result.isRight)
      assertResult(SchemaMetadata.instance(id, Schema.create(Schema.Type.STRING).toString, Utils.toMD5Hex(Schema.create(Schema.Type.STRING).toString), SchemaType.AVRO))(result.right.get)
    }
  }

  "SchemaIdBySubjectAndSchema" should {
    "return SchemaIsNotValid" in {
      val result = schemaStorage.schemaIdBySubjectAndSchema("A1", "not valid schema")
      assert(result.isLeft)
      assertResult(SchemaIsNotValid("not valid schema"))(result.left.get)
    }

    "return SchemaIsNotRegistered" in {
      val result = schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      assert(result.isLeft)
      assertResult(SchemaIsNotRegistered(Schema.create(Schema.Type.STRING).toString()))(result.left.get)
    }

    "return SubjectIsNotConnectedToSchema" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO).right.get.getSchemaId
      val result = schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      assert(result.isLeft)
      assertResult(SubjectIsNotConnectedToSchema("A1", id))(result.left.get)
    }

    "return SchemaId" in {
      val schemaId = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.BACKWARD, SchemaType.AVRO).right.get
      val result = schemaStorage.schemaIdBySubjectAndSchema("A1", Schema.create(Schema.Type.STRING).toString())
      assert(result.isRight)
      assertResult(schemaId)(result.right.get)
    }
  }

  "DeleteSubject" should {
    "return false" when {
      "subject does not exist" in {
        val result = schemaStorage.deleteSubject("A1")
        assert(result.isRight)
        assertResult(false)(result.right.get)
      }
    }

    "return true" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.deleteSubject("A1")
      assert(result.isRight)
      assertResult(true)(result.right.get)
    }
  }

  "DeleteSubjectSchemaByVersion" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return SubjectSchemaVersionDoesNotExist" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      assert(result.isLeft)
      assertResult(service.SubjectSchemaVersionDoesNotExist("A1", 1))(result.left.get)
    }

    "return true" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      val result = schemaStorage.deleteSubjectSchemaByVersion("A1", 1)
      assert(result.isRight)
      assertResult(true)(result.right.get)
    }
  }

  "CheckSubjectCompatibility" should {
    "return SchemaIsNotValid" in {
      val result = schemaStorage.checkSubjectSchemaCompatibility("A1", "SCHEMA")
      assert(result.isLeft)
      assertResult(SchemaIsNotValid("SCHEMA"))(result.left.get)
    }

    "return SubjectDoesNotExist" in {
      val result = schemaStorage.checkSubjectSchemaCompatibility("A1", Schema.create(Schema.Type.STRING).toString())
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return true" in {
      val schema1 = SchemaBuilder
        .builder()
        .record("test")
        .fields()
        .requiredString("f1")
        .endRecord()
      val schema2 = SchemaBuilder
        .builder()
        .record("test")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()

      schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.BACKWARD, SchemaType.AVRO)
      val result = schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())

      assert(result.isRight)
      assertResult(true)(result.right.get)
    }

    "return false" in {
      val schema1 = SchemaBuilder
        .builder()
        .record("test")
        .fields()
        .requiredString("f1")
        .endRecord()
      val schema2 = SchemaBuilder
        .builder()
        .record("test")
        .fields()
        .optionalString("f2")
        .endRecord()

      schemaStorage.registerSchema("A1", schema1.toString(), CompatibilityType.FORWARD, SchemaType.AVRO)
      val result = schemaStorage.checkSubjectSchemaCompatibility("A1", schema2.toString())

      assert(result.isRight)
      assertResult(false)(result.right.get)
    }
  }

  "GetSubjectSchemas" should {
    "return SubjectDoesNotExist" in {
      val result = schemaStorage.getSubjectSchemas("A1")
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return SubjectHasNoRegisteredSchemas" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.getSubjectSchemas("A1")
      assert(result.isLeft)
      assertResult(SubjectHasNoRegisteredSchemas("A1"))(result.left.get)
    }

    "return last schema" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.BACKWARD, SchemaType.AVRO)
      val result = schemaStorage.getSubjectSchemas("A1")
      assert(result.isRight)
      assert(result.right.get.size == 1)
    }
  }

  "RegisterSchema (only)" should {
    "do not register new schema due to schema is not a valid avro schema" in {
      val result = schemaStorage.registerSchema("SCHEMA", SchemaType.AVRO)
      assert(result.isLeft)
      assertResult(SchemaIsNotValid("SCHEMA"))(result.left.get)
    }

    "register new schema and return schemaId" in {
      val result = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
      assert(result.isRight)
    }

    "does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema" in {
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).right.get.getSchemaId
      val result = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
      assert(result.isLeft)
      assertResult(SchemaIsAlreadyExist(id, Schema.create(Schema.Type.STRING).toString))(result.left.get)
    }
  }

  "RegisterSchema and subject if does not exist and connect to each other" should {
    "return SubjectIsLocked" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
      val result = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      assert(result.isLeft)
      assertResult(SubjectIsLocked("A1"))(result.left.get)
    }

    "do not register new schema and subject due to schema is not a valid avro schema" in {
      val result = schemaStorage.registerSchema("A1", "SCHEMA", CompatibilityType.BACKWARD, SchemaType.AVRO)
      assert(result.isLeft)
      assertResult(SchemaIsNotValid("SCHEMA"))(result.left.get)
      assert(schemaStorage.subjects().right.get.isEmpty)
    }

    "register subject, register new schema and return schemaId" in {
      val result = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      assert(result.isRight)
      assert(schemaStorage.subjects().right.get.size == 1)
      assertResult(result.right.get.getSchemaId)(schemaStorage.getSubjectSchemas("A1").right.get.head.getSchemaId)
    }

    "register subject, does not register new schema (because schema with the same hash is already exist) and return schema id of existing schema" in {
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO).right.get.getSchemaId
      val result = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      assert(result.isRight)
      assert(schemaStorage.subjects().right.get.size == 1)
      assertResult(result.right.get.getSchemaId)(id)
    }

    "does not register schema and subject and does not connect schema and subject because they are already connected" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
      schemaStorage.addSchemaToSubject("A1", id.right.get.getSchemaId)
      val result = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      assert(result.isLeft)
      assertResult(SubjectIsAlreadyConnectedToSchema("A1", id.right.get.getSchemaId))(result.left.get)
    }

    "does not connect schema and subject because new schema is not compatible" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
      // CompatibilityType should not be changed from BACKWARD to NONE
      val result = schemaStorage.registerSchema("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.NONE, SchemaType.AVRO)
      assert(result.isLeft)
      assertResult(SchemaIsNotCompatible("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.BACKWARD))(result.left.get)
    }
  }

  "RegisterSubject" should {
    "register new subject" in {
      val before = schemaStorage.subjects().right.get
      val result = schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val after = schemaStorage.subjects().right.get

      assert(before.isEmpty)
      assertResult(after)(List("A1"))
      assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))(result.right.get)
    }

    "register new locked subject" in {
      val before = schemaStorage.subjects().right.get
      val result = schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
      val after = schemaStorage.subjects().right.get

      assert(before.isEmpty)
      assertResult(after)(List("A1"))
      assertResult(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true))(result.right.get)
    }

    "return SubjectIsAlreadyExists" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      assert(result.isLeft)
      assertResult(SubjectIsAlreadyExists("A1"))(result.left.get)
    }
  }

  "AddSchemaToSubject" should {
    "successfully add schema to subject" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO).right.get.getSchemaId
      val result = schemaStorage.addSchemaToSubject("A1", id)
      assert(result.isRight)
      // 1 - version
      assertResult(1)(result.right.get)
    }

    "return SchemaIsNotCompatible" in {
      schemaStorage.registerSchema("A1", Schema.create(Schema.Type.INT).toString(), CompatibilityType.BACKWARD, SchemaType.AVRO)
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO).right.get.getSchemaId
      val result = schemaStorage.addSchemaToSubject("A1", id)
      assert(result.isLeft)
      assertResult(SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString(), CompatibilityType.BACKWARD))(result.left.get)
    }

    "return SubjectDoesNotExist" in {
      val result = schemaStorage.addSchemaToSubject("A1", 1)
      assert(result.isLeft)
      assertResult(SubjectDoesNotExist("A1"))(result.left.get)
    }

    "return SubjectIsAlreadyConnectedToSchema" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val id = schemaStorage.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO).right.get.getSchemaId
      schemaStorage.addSchemaToSubject("A1", id)
      val result = schemaStorage.addSchemaToSubject("A1", id)
      assert(result.isLeft)
      assertResult(SubjectIsAlreadyConnectedToSchema("A1", id))(result.left.get)
    }

    "return SchemaDoesNotExist" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
      val result = schemaStorage.addSchemaToSubject("A1", 1)
      assert(result.isLeft)
      assertResult(SchemaIdDoesNotExist(1))(result.left.get)
    }

    "return SubjectIsLocked" in {
      schemaStorage.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
      val result = schemaStorage.addSchemaToSubject("A1", 1)
      assert(result.isLeft)
      assertResult(SubjectIsLocked("A1"))(result.left.get)
    }
  }
}

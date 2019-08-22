package schemakeeper.server.avro

import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaCompatibilityTest extends WordSpec with Matchers {
  "NONE compatibility validator" should {
    val validator = AvroSchemaCompatibility.NONE_VALIDATOR

    "always return true" in {
      val a = Schema.create(Schema.Type.INT)
      val b = Schema.create(Schema.Type.STRING)
      val c = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .optionalString("f1")
        .requiredInt("f2")
        .optionalDouble("f3")
        .nullableString("f5", "default")
        .endRecord()

      assert(validator.isCompatible(a, b))
      assert(validator.isCompatible(b, c))
      assert(validator.isCompatible(a, c))
    }
  }

  "BACKWARD compatibility validator" should {
    val validator = AvroSchemaCompatibility.BACKWARD_VALIDATOR

    "return true - add new optional field and remove one required and one optional" in {
      val previousSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .optionalString("f3")
        .endRecord()

      assert(validator.isCompatible(newSchema, previousSchema))
    }

    "return false - add new required field" in {
      val previousSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .requiredString("f3")
        .endRecord()

      assert(!validator.isCompatible(newSchema, previousSchema))
    }

    "return true - nested field type" in {
      val firstSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val secondSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .nullableString("f2", "default")
        .endRecord()
      val thirdSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .nullableString("f2", "default")
        .name("nested")
          .`type`()
          .optional()
          .record("nested")
          .fields()
          .optionalString("nested_f1")
          .endRecord()
        .endRecord()

      assert(validator.isCompatible(secondSchema, firstSchema))
      assert(validator.isCompatible(thirdSchema, secondSchema))
    }
  }

  "FORWARD compatibility validator" should {
    val validator = AvroSchemaCompatibility.FORWARD_VALIDATOR

    "return true - delete optional field and add required and optional fields" in {
      val previousSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .requiredString("f3")
        .optionalString("f4")
        .endRecord()

      assert(validator.isCompatible(newSchema, previousSchema))
    }

    "return false - delete required field" in {
      val previousSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .optionalString("f2")
        .endRecord()

      assert(!validator.isCompatible(newSchema, previousSchema))
    }
  }

  "FULL compatibility validator - add optional field" should {
    val validator = AvroSchemaCompatibility.FULL_VALIDATOR

    "return true" in {
      val previousSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .optionalString("f3")
        .endRecord()

      assert(validator.isCompatible(newSchema, previousSchema))
    }

    "return false - delete required field" in {
      val previousSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .optionalString("f2")
        .endRecord()

      assert(!validator.isCompatible(newSchema, previousSchema))
    }
  }

  "BACKWARD TRANSITIVE compatibility validator" should {
    val validator = AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR

    "return true - add optional fields" in {
      val previousSchema1 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val previousSchema2 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .optionalString("f3")
        .endRecord()

      assert(validator.isCompatible(newSchema, Seq(previousSchema1, previousSchema2)))
    }

    "return false - add required field" in {
      val previousSchema1 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val previousSchema2 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .optionalString("f3")
        .requiredString("f4")
        .endRecord()

      assert(!validator.isCompatible(newSchema, Seq(previousSchema1, previousSchema2)))
    }
  }

  "FORWARD TRANSITIVE compatibility validator" should {
    val validator = AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR

    "return true - add required and remove optional fields" in {
      val previousSchema1 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val previousSchema2 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .requiredString("f3")
        .endRecord()

      assert(validator.isCompatible(newSchema, Seq(previousSchema1, previousSchema2)))
    }

    "return false - delete required" in {
      val previousSchema1 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val previousSchema2 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f3")
        .endRecord()

      assert(!validator.isCompatible(newSchema, Seq(previousSchema1, previousSchema2)))
    }
  }

  "FULL TRANSITIVE compatibility validator" should {
    val validator = AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR

    "return true - add optional fields" in {
      val previousSchema1 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val previousSchema2 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .optionalString("f3")
        .endRecord()

      assert(validator.isCompatible(newSchema, Seq(previousSchema1, previousSchema2)))
    }

    "return false - add required field" in {
      val previousSchema1 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .endRecord()
      val previousSchema2 = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .endRecord()
      val newSchema = SchemaBuilder
        .builder("namespace")
        .record("record")
        .fields()
        .requiredString("f1")
        .optionalString("f2")
        .requiredString("f3")
        .endRecord()

      assert(!validator.isCompatible(newSchema, Seq(previousSchema1, previousSchema2)))
    }
  }
}

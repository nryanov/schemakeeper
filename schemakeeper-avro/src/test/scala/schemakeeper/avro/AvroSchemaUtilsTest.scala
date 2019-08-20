package schemakeeper.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaUtilsTest extends WordSpec with Matchers {
  "AvroUtils" should {
    "return schema" when {
      "object is string" in {
        val value: String = ""
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.STRING))(schema)
      }

      "object is bytes" in {
        val value: Array[Byte] = Array()
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.BYTES))(schema)
      }

      "object is int" in {
        val value: Int = 0
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.INT))(schema)
      }

      "object is long" in {
        val value: Long = 0
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.LONG))(schema)
      }

      "object is float" in {
        val value: Float = 0
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.FLOAT))(schema)
      }

      "object is double" in {
        val value: Double = 0
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.DOUBLE))(schema)
      }

      "object is boolean" in {
        val value: Boolean = true
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.BOOLEAN))(schema)
      }

      "object is null" in {
        val value: AnyRef = null
        val schema = AvroSchemaUtils.getSchema(value)
        assertResult(Schema.create(Schema.Type.NULL))(schema)
      }

      "object is generic" in {
        val schema = SchemaBuilder
          .builder("test_namespace")
          .record("test")
          .fields()
          .requiredString("f1")
          .endRecord()
        val record = new GenericData.Record(schema)
        assertResult(schema)(AvroSchemaUtils.getSchema(record))
      }
    }

    "check if object has primitive schema" when {
      "object is string" in {
        val value: String = ""
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is bytes" in {
        val value: Array[Byte] = Array()
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is int" in {
        val value: Int = 0
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is long" in {
        val value: Long = 0
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is float" in {
        val value: Float = 0
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is double" in {
        val value: Double = 0
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is boolean" in {
        val value: Boolean = true
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is null" in {
        val value: AnyRef = null
        assert(AvroSchemaUtils.isPrimitive(value))
      }

      "object is generic" in {
        val schema = SchemaBuilder
          .builder("test_namespace")
          .record("test")
          .fields()
          .requiredString("f1")
          .endRecord()
        val record = new GenericData.Record(schema)
        assert(!AvroSchemaUtils.isPrimitive(record))
      }
    }

    "throw error while getting schema" in {
      assertThrows[IllegalArgumentException](AvroSchemaUtils.getSchema(Array[String]()))
    }

    "throw error while checking object type" in {
      assertThrows[IllegalArgumentException](AvroSchemaUtils.isPrimitive(Array[String]()))
    }
  }
}

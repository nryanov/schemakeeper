package schemakeeper.avro


import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

object AvroSchemaUtils {
  private val primitiveTypes = Map[Schema.Type, Schema](
    Schema.Type.STRING -> Schema.create(Schema.Type.STRING),
    Schema.Type.BYTES -> Schema.create(Schema.Type.BYTES),
    Schema.Type.INT -> Schema.create(Schema.Type.INT),
    Schema.Type.LONG -> Schema.create(Schema.Type.LONG),
    Schema.Type.FLOAT -> Schema.create(Schema.Type.FLOAT),
    Schema.Type.DOUBLE -> Schema.create(Schema.Type.DOUBLE),
    Schema.Type.BOOLEAN -> Schema.create(Schema.Type.BOOLEAN),
    Schema.Type.NULL -> Schema.create(Schema.Type.NULL)
  )

  def getSchema(value: Any): Schema = value match {
    case null => primitiveTypes(Schema.Type.NULL)
    case _: String => primitiveTypes(Schema.Type.STRING)
    case _: Int => primitiveTypes(Schema.Type.INT)
    case _: Short => primitiveTypes(Schema.Type.INT)
    case _: Byte => primitiveTypes(Schema.Type.INT)
    case _: Long => primitiveTypes(Schema.Type.LONG)
    case _: Float => primitiveTypes(Schema.Type.FLOAT)
    case _: Double => primitiveTypes(Schema.Type.DOUBLE)
    case _: Boolean => primitiveTypes(Schema.Type.BOOLEAN)
    case _: Array[Byte] => primitiveTypes(Schema.Type.BYTES)
    case c: GenericContainer => c.getSchema
    case _ => throw new IllegalArgumentException("Unsupported avro type")
  }

  def isPrimitive(value: Any): Boolean = value match {
    case null => true
    case _: String => true
    case _: Int => true
    case _: Short => true
    case _: Byte => true
    case _: Long => true
    case _: Float => true
    case _: Double => true
    case _: Boolean => true
    case _: Array[Byte] => true
    case _: GenericContainer => false
    case _ => throw new IllegalArgumentException("Unsupported avro type")
  }

  def isPrimitive(schema: Schema): Boolean = primitiveTypes.contains(schema.getType)
}

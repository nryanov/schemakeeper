package schemakeeper.client

import java.{lang, util}

import org.apache.avro.Schema
import org.slf4j.{Logger, LoggerFactory}
import schemakeeper.schema.{AvroSchemaCompatibility, CompatibilityType}
import scala.collection.JavaConverters._

class InMemorySchemaKeeperClient(compatibilityType: CompatibilityType = CompatibilityType.None) extends SchemaKeeperClient {
  def this(lvl: String) {
    this(CompatibilityType.withNameInsensitive(lvl))
  }

  private val logger: Logger = LoggerFactory.getLogger(classOf[InMemorySchemaKeeperClient])

  private var id: Int = 0
  private val ID_SCHEMA: util.Map[Integer, Schema] = new util.HashMap[Integer, Schema]
  private val SCHEMA_ID: util.Map[Schema, Integer] = new util.HashMap[Schema, Integer]
  private val SUBJECT_SCHEMAS: util.Map[String, util.Map[Integer, Schema]] = new util.HashMap[String, util.Map[Integer, Schema]]

  override def getSchemaById(id: Int): Schema = {
    logger.debug("Get schema by id: {}", id)
    ID_SCHEMA.get(id)
  }

  override def getSubjectSchemas(subject: String): lang.Iterable[Schema] = {
    logger.debug("Return schema by subject name: {}", subject)

    if (SUBJECT_SCHEMAS.containsKey(subject)) {
      if (SUBJECT_SCHEMAS.get(subject).values != null) {
        SUBJECT_SCHEMAS.get(subject).values
      } else {
        List.empty[Schema].asJava
      }
    } else {
      List.empty[Schema].asJava
    }
  }

  override def getSchemaId(schema: Schema): Int = {
    logger.debug("Get schema id for schema: {}", schema)
    SCHEMA_ID.getOrDefault(schema, -1)
  }

  override def getLastSubjectSchema(subject: String): Schema = {
    logger.debug(s"Get last schema for subject: $subject")
    val schemas = SUBJECT_SCHEMAS.get(subject)

    if (schemas == null) {
      null
    } else {
      schemas.values().asScala.last
    }
  }

  override def registerNewSchema(subject: String, schema: Schema): Int = {
    logger.debug(s"Register new schema for subject: $subject, ${schema.toString()}")

    val isCompatible = compatibilityType match {
      case CompatibilityType.None => true
      case CompatibilityType.Backward => AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(schema, getLastSubjectSchema(subject))
      case CompatibilityType.Forward => AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(schema, getLastSubjectSchema(subject))
      case CompatibilityType.Full => AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(schema, getLastSubjectSchema(subject))
      case CompatibilityType.BackwardTransitive => AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(schema, getSubjectSchemas(subject).asScala.toSeq)
      case CompatibilityType.ForwardTransitive => AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(schema, getSubjectSchemas(subject).asScala.toSeq)
      case CompatibilityType.FullTransitive => AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(schema, getSubjectSchemas(subject).asScala.toSeq)
    }

    logger.debug(s"$schema")
    logger.debug(s"${getLastSubjectSchema(subject)}")
    if (!isCompatible) {
      throw new RuntimeException("New schema is not compatible")
    }

    id += 1
    ID_SCHEMA.put(id, schema)
    SCHEMA_ID.put(schema, id)

    if (SUBJECT_SCHEMAS.get(subject) != null) {
      SUBJECT_SCHEMAS.get(subject).put(id, schema)
    } else {
      val schemas = new util.HashMap[Integer, Schema]
      schemas.put(id, schema)
      SUBJECT_SCHEMAS.put(subject, schemas)
    }

    id
  }
}

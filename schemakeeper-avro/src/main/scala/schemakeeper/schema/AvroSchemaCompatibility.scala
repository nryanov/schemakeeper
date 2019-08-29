package schemakeeper.schema

import java.lang

import org.apache.avro.{Schema, SchemaValidationException, SchemaValidator, SchemaValidatorBuilder}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class AvroSchemaCompatibility private(validator: SchemaValidator) {
  def isCompatible(newSchema: Schema, previousSchema: Schema): Boolean = {
    if (previousSchema == null) {
      true
    } else {
      isCompatible(newSchema, Seq(previousSchema))
    }
  }

  def isCompatible(newSchema: Schema, previousSchemas: Seq[Schema]): Boolean = Try {
    validator.validate(newSchema, previousSchemas.reverse.asJava)
  } match {
    case Success(_) => true
    case Failure(_: SchemaValidationException) => false
    case Failure(exception) => throw exception // should never happen
  }
}

object AvroSchemaCompatibility {
  val NONE_VALIDATOR = new AvroSchemaCompatibility((_: Schema, _: lang.Iterable[Schema]) => Unit)
  val BACKWARD_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canReadStrategy.validateLatest())
  val FORWARD_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canBeReadStrategy.validateLatest())
  val FULL_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().mutualReadStrategy().validateLatest())
  val BACKWARD_TRANSITIVE_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canReadStrategy.validateAll())
  val FORWARD_TRANSITIVE_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canBeReadStrategy.validateAll())
  val FULL_TRANSITIVE_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().mutualReadStrategy.validateAll())
}

package schemakeeper.server.avro

import enumeratum._

import scala.collection.immutable

sealed abstract class AvroSchemaCompatibilityType extends EnumEntry

object AvroSchemaCompatibilityType extends Enum[AvroSchemaCompatibilityType] {
  override def values: immutable.IndexedSeq[AvroSchemaCompatibilityType] = findValues

  case object None extends AvroSchemaCompatibilityType

  case object Backward extends AvroSchemaCompatibilityType

  case object Forward extends AvroSchemaCompatibilityType

  case object Full extends AvroSchemaCompatibilityType

  case object BackwardTransitive extends AvroSchemaCompatibilityType

  case object ForwardTransitive extends AvroSchemaCompatibilityType

  case object FullTransitive extends AvroSchemaCompatibilityType
}

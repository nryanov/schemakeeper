package schemakeeper.avro.compatibility

import enumeratum.EnumEntry.Snakecase
import enumeratum._

import scala.collection.immutable

sealed abstract class CompatibilityType extends EnumEntry with Snakecase

object CompatibilityType extends Enum[CompatibilityType] {
  override def values: immutable.IndexedSeq[CompatibilityType] = findValues

  case object None extends CompatibilityType

  case object Backward extends CompatibilityType

  case object Forward extends CompatibilityType

  case object Full extends CompatibilityType

  case object BackwardTransitive extends CompatibilityType

  case object ForwardTransitive extends CompatibilityType

  case object FullTransitive extends CompatibilityType
}

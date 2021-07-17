package schemakeeper.server.datasource.migration

import enumeratum._

import scala.collection.immutable

sealed abstract class SupportedDatabaseProvider extends EnumEntry

object SupportedDatabaseProvider extends Enum[SupportedDatabaseProvider] {
  override def values: immutable.IndexedSeq[SupportedDatabaseProvider] =
    findValues

  case object PostgreSQL extends SupportedDatabaseProvider

  case object MySQL extends SupportedDatabaseProvider

  case object H2 extends SupportedDatabaseProvider

  case object MariaDB extends SupportedDatabaseProvider

  case object Oracle extends SupportedDatabaseProvider
}

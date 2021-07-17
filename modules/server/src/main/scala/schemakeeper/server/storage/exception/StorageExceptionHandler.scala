package schemakeeper.server.storage.exception

import schemakeeper.server.Configuration
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider
import schemakeeper.server.datasource.DataSourceUtils

trait StorageExceptionHandler {
  def isUniqueViolation(e: Throwable): Boolean
}

object StorageExceptionHandler {
  def apply(config: Configuration): StorageExceptionHandler =
    DataSourceUtils.detectDatabaseProvider(config.storage.url) match {
      case SupportedDatabaseProvider.H2         => H2ExceptionHandler()
      case SupportedDatabaseProvider.PostgreSQL => PostgreSQLExceptionHandler()
      case SupportedDatabaseProvider.MySQL      => MySQLExceptionHandler()
      case SupportedDatabaseProvider.MariaDB    => MariaDBExceptionHandler()
      case SupportedDatabaseProvider.Oracle     => OracleExceptionHandler()
    }
}

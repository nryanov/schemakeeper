package schemakeeper.server.storage.exception

import schemakeeper.server.Configuration
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider
import schemakeeper.server.datasource.{DataSource, DataSourceUtils}

trait StorageExceptionHandler {
  def isRecoverable(e: Throwable): Boolean
}

object StorageExceptionHandler {
  def apply(config: Configuration): StorageExceptionHandler = DataSourceUtils.detectDatabaseProvider(config.databaseConnectionString) match {
    case SupportedDatabaseProvider.H2 => H2ExceptionHandler()
    case SupportedDatabaseProvider.PostgreSQL => PostgreSQLExceptionHandler()
    case SupportedDatabaseProvider.MySQL => MySQLExceptionHandler()
  }
}
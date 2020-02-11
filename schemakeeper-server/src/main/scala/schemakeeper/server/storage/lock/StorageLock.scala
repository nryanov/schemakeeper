package schemakeeper.server.storage.lock

import doobie.free.connection.ConnectionIO
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSourceUtils
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

trait StorageLock[F[_]] {
  def lockForUpdate(): F[Unit]
}

object StorageLock {
  def apply(config: Configuration): StorageLock[ConnectionIO] =
    DataSourceUtils.detectDatabaseProvider(config.storage.url) match {
      case SupportedDatabaseProvider.H2         => H2StorageLock()
      case SupportedDatabaseProvider.PostgreSQL => PostgreSQLStorageLock()
      case SupportedDatabaseProvider.MySQL      => MySQLStorageLock()
      case SupportedDatabaseProvider.MariaDB    => MariaDBStorageLock()
      case SupportedDatabaseProvider.Oracle     => OracleStorageLock()
    }
}

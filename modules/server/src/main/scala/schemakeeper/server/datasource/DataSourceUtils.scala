package schemakeeper.server.datasource

import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

object DataSourceUtils {
  def detectDatabaseProvider(connectionString: String): SupportedDatabaseProvider = connectionString match {
    case cs if cs.startsWith("jdbc:postgresql") => SupportedDatabaseProvider.PostgreSQL
    case cs if cs.startsWith("jdbc:mysql")      => SupportedDatabaseProvider.MySQL
    case cs if cs.startsWith("jdbc:h2")         => SupportedDatabaseProvider.H2
    case cs if cs.startsWith("jdbc:mariadb")    => SupportedDatabaseProvider.MariaDB
    case cs if cs.startsWith("jdbc:oracle")     => SupportedDatabaseProvider.Oracle
    case _                                      => throw new IllegalArgumentException("Unsupported database provider")
  }
}

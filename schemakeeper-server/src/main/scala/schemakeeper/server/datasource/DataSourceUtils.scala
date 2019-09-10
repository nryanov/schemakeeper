package schemakeeper.server.datasource

import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

object DataSourceUtils {
  def detectDatabaseProvider(connectionString: String): SupportedDatabaseProvider = connectionString match {
    case cs if cs.startsWith("jdbc:postgresql") => SupportedDatabaseProvider.PostgreSQL
    case cs if cs.startsWith("jdbc:mysql") => SupportedDatabaseProvider.MySQL
    case cs if cs.startsWith("jdbc:h2") => SupportedDatabaseProvider.H2
    case _ => throw new RuntimeException("Unsupported database provider")
  }
}

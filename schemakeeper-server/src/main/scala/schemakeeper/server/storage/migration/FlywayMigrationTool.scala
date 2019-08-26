package schemakeeper.server.storage.migration

import org.flywaydb.core.Flyway
import org.flywaydb.core.internal.configuration.ConfigUtils
import schemakeeper.server.Configuration

import scala.collection.JavaConverters._

object FlywayMigrationTool {
  def migrate(configuration: Configuration): Unit = {
    val flyway = Flyway
      .configure()
      .configuration(
        Map(
          ConfigUtils.LOCATIONS -> getMigrationLocation(detectDatabaseProvider(configuration.databaseConnectionString)),
          ConfigUtils.SCHEMAS -> "schemakeeper"
        ).asJava
      )
      .dataSource(configuration.databaseConnectionString, configuration.databaseUsername, configuration.databasePassword)
      .load()

    flyway.migrate()
  }

  private def detectDatabaseProvider(connectionString: String): SupportedDatabaseProvider = connectionString match {
    case cs if cs.startsWith("jdbc:postgresql") => SupportedDatabaseProvider.PostgreSQL
    case cs if cs.startsWith("jdbc:mysql") => SupportedDatabaseProvider.MySQL
    case _ => throw new RuntimeException("Unsupported database provider")
  }

  private def getMigrationLocation(provider: SupportedDatabaseProvider): String = provider match {
    case SupportedDatabaseProvider.PostgreSQL => "db/migration/postgresql"
    case SupportedDatabaseProvider.MySQL => "db/migration/mysql"
  }
}

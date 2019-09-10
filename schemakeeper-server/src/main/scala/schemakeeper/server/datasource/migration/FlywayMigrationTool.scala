package schemakeeper.server.datasource.migration

import org.flywaydb.core.Flyway
import org.flywaydb.core.internal.configuration.ConfigUtils
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSourceUtils

import scala.collection.JavaConverters._

object FlywayMigrationTool {
  def migrate(configuration: Configuration): Unit = {
    val flyway = Flyway
      .configure()
      .configuration(
        Map(
          ConfigUtils.LOCATIONS -> getMigrationLocation(DataSourceUtils.detectDatabaseProvider(configuration.databaseConnectionString)),
          ConfigUtils.SCHEMAS -> configuration.databaseSchema,
          s"${ConfigUtils.PLACEHOLDERS_PROPERTY_PREFIX}schemakeeper_schema" -> configuration.databaseSchema
        ).asJava
      )
      .dataSource(configuration.databaseConnectionString, configuration.databaseUsername, configuration.databasePassword)
      .load()

    flyway.migrate()
  }

  private def getMigrationLocation(provider: SupportedDatabaseProvider): String = provider match {
    case SupportedDatabaseProvider.PostgreSQL => "db/migration/postgresql"
    case SupportedDatabaseProvider.MySQL => "db/migration/mysql"
    case SupportedDatabaseProvider.H2 => "db/migration/h2"
  }
}

package schemakeeper.server.datasource.migration

import org.flywaydb.core.Flyway
import org.flywaydb.core.internal.configuration.ConfigUtils
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSourceUtils

import scala.collection.JavaConverters._

object FlywayMigrationTool {
  def build(configuration: Configuration): Flyway = Flyway
    .configure()
    .configuration(
      Map(
        ConfigUtils.LOCATIONS -> getMigrationLocation(
          DataSourceUtils.detectDatabaseProvider(configuration.storage.url)
        ),
        ConfigUtils.SCHEMAS -> configuration.storage.schema,
        s"${ConfigUtils.PLACEHOLDERS_PROPERTY_PREFIX}schemakeeper_schema" -> configuration.storage.schema
      ).asJava
    )
    .dataSource(
      configuration.storage.url,
      configuration.storage.username,
      configuration.storage.password
    )
    .load()

  private def getMigrationLocation(
    provider: SupportedDatabaseProvider
  ): String = provider match {
    case SupportedDatabaseProvider.PostgreSQL => "db/migration/postgresql"
    case SupportedDatabaseProvider.MySQL      => "db/migration/mysql"
    case SupportedDatabaseProvider.H2         => "db/migration/h2"
    case SupportedDatabaseProvider.MariaDB    => "db/migration/mariadb"
    case SupportedDatabaseProvider.Oracle     => "db/migration/oracle"
  }
}

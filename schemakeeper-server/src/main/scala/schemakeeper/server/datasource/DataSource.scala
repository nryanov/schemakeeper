package schemakeeper.server.datasource

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariConfig
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.quill.{DoobieContext, DoobieContextBase}
import doobie.util.transactor.Transactor
import io.getquill.{NamingStrategy, SnakeCase}
import io.getquill.context.sql.idiom.SqlIdiom
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

import scala.concurrent.ExecutionContext.Implicits.global

object DataSource {
  def build(config: Configuration): Resource[IO, Transactor[IO]] = {
    implicit val cs = IO.contextShift(global)

    val cfg = new HikariConfig()
    cfg.setMaximumPoolSize(config.databaseMaxConnections)
    cfg.setDriverClassName(config.databaseDriver)
    cfg.setJdbcUrl(config.databaseConnectionString)
    cfg.setUsername(config.databaseUsername)
    cfg.setPassword(config.databasePassword)
    cfg.setSchema(config.databaseSchema)
    cfg.setMinimumIdle(1)

    for {
      connectionExecutionPool <- ExecutionContexts.fixedThreadPool[IO](Runtime.getRuntime.availableProcessors())
      transactionExecutionPool <- ExecutionContexts.cachedThreadPool[IO]
      xa <- HikariTransactor.fromHikariConfig[IO](
        cfg,
        connectionExecutionPool,
        transactionExecutionPool
      )
    } yield xa
  }

  def context(configuration: Configuration): DoobieContextBase[_ <: SqlIdiom, _ <: NamingStrategy] =
    DataSourceUtils.detectDatabaseProvider(configuration.databaseConnectionString) match {
      case SupportedDatabaseProvider.PostgreSQL => new DoobieContext.Postgres(SnakeCase)
      case SupportedDatabaseProvider.MySQL      => new DoobieContext.MySQL(SnakeCase)
      case SupportedDatabaseProvider.H2         => new DoobieContext.H2(SnakeCase)
      // currently, doobie-quill has no explicit MariaDB context
      case SupportedDatabaseProvider.MariaDB => new DoobieContext.MySQL(SnakeCase)
      case SupportedDatabaseProvider.Oracle  => new DoobieContext.Oracle(SnakeCase)
    }
}

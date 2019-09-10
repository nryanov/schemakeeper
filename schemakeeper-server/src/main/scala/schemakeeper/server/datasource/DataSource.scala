package schemakeeper.server.datasource

import cats.effect.{IO, Resource}
import com.zaxxer.hikari.HikariConfig
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor
import schemakeeper.server.Configuration

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
}

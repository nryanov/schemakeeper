package schemakeeper.server.datasource

import cats.arrow.FunctionK
import cats.effect.{Async, Blocker, ContextShift, Resource}
import com.zaxxer.hikari.HikariConfig
import doobie.ExecutionContexts
import doobie.h2.H2Transactor
import doobie.hikari.HikariTransactor
import doobie.quill.{DoobieContext, DoobieContextBase}
import doobie.util.transactor.Transactor
import doobie.implicits._
import io.getquill.{NamingStrategy, SnakeCase}
import io.getquill.context.sql.idiom.SqlIdiom
import schemakeeper.server.{Configuration, Storage}
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

object DataSource {
  def context(cfg: Configuration): DoobieContextBase[_ <: SqlIdiom, _ <: NamingStrategy] =
    DataSourceUtils.detectDatabaseProvider(cfg.storage.url) match {
      case SupportedDatabaseProvider.PostgreSQL => new DoobieContext.Postgres(SnakeCase)
      case SupportedDatabaseProvider.MySQL      => new DoobieContext.MySQL(SnakeCase)
      case SupportedDatabaseProvider.H2         => new DoobieContext.H2(SnakeCase)
      // currently, doobie-quill has no explicit MariaDB context
      case SupportedDatabaseProvider.MariaDB => new DoobieContext.MySQL(SnakeCase)
      case SupportedDatabaseProvider.Oracle  => new DoobieContext.Oracle(SnakeCase)
    }

  def resource[F[_]: Async: ContextShift](config: Configuration): Resource[F, FunctionK[doobie.ConnectionIO, F]] = {
    val transactor: Resource[F, Transactor[F]] = DataSourceUtils.detectDatabaseProvider(config.storage.url) match {
      case SupportedDatabaseProvider.H2 => hikariTransactor(config)
      case _                            => hikariTransactor(config)
    }

    transactor.map { tx =>
      def transact[A](tx: Transactor[F])(sql: doobie.ConnectionIO[A]): F[A] =
        sql.transact(tx)

      new FunctionK[doobie.ConnectionIO, F] {
        def apply[A](l: doobie.ConnectionIO[A]): F[A] = transact(tx)(l)
      }
    }
  }

  private def hikariTransactor[F[_]: Async: ContextShift](config: Configuration): Resource[F, Transactor[F]] =
    for {
      connectionExecutionPool <- ExecutionContexts.fixedThreadPool[F](Runtime.getRuntime.availableProcessors())
      transactionExecutionPool <- Blocker[F]
      cfg = hikariConfig(config.storage)
      xa <- HikariTransactor.fromHikariConfig[F](
        cfg,
        connectionExecutionPool,
        transactionExecutionPool
      )
    } yield xa

  private def h2Transactor[F[_]: Async: ContextShift](config: Configuration): Resource[F, Transactor[F]] =
    for {
      connectionExecutionPool <- ExecutionContexts.fixedThreadPool[F](Runtime.getRuntime.availableProcessors())
      transactionExecutionPool <- Blocker[F]
      xa <- H2Transactor.newH2Transactor[F](
        url = config.storage.url,
        user = config.storage.username,
        pass = config.storage.password,
        connectionExecutionPool,
        transactionExecutionPool
      )
    } yield xa

  private def hikariConfig(storage: Storage): HikariConfig = {
    val cfg = new HikariConfig()
    cfg.setMaximumPoolSize(storage.maxConnections)
    cfg.setDriverClassName(storage.driver)
    cfg.setJdbcUrl(storage.url)
    cfg.setUsername(storage.username)
    cfg.setPassword(storage.password)
    cfg.setSchema(storage.schema)
    cfg.setMinimumIdle(1)

    cfg
  }
}

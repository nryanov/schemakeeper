package schemakeeper.server

import cats.effect.{Async, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Sync, Timer}
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import org.http4s.server.blaze._
import doobie.free.connection.ConnectionIO
import doobie.quill.DoobieContextBase
import io.getquill.NamingStrategy
import io.getquill.context.sql.idiom.SqlIdiom
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.datasource.migration.FlywayMigrationTool
import schemakeeper.server.http.{SchemaKeeperApi, SchemaKeeperRouter, SwaggerApi}
import schemakeeper.server.service.DBBackedService
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.storage.exception.StorageExceptionHandler
import schemakeeper.server.storage.lock.StorageLock

object SchemaKeeper extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    commonSettings[IO]()
      .evalTap(common => migrate[IO](common.cfg))
      .flatMap(common => applicationServer[IO](common))
      .use { server =>
        server.serve.compile.drain.as(ExitCode.Success)
      }

  private def applicationServer[F[_]: Async: ContextShift: ConcurrentEffect: Timer](
    common: Common
  ): Resource[F, BlazeServerBuilder[F]] = for {
    transact <- DataSource.resource(common.cfg)
    storage <- Resource.pure(DatabaseStorage.create(common.context, common.exceptionHandler))
    service <- Resource.pure(DBBackedService.create(storage, transact, common.lock))
    schemakeeperApi <- Resource.pure(SchemaKeeperApi.create(service))
    swaggerApi <- Resource.pure(SwaggerApi.create(schemakeeperApi))
    server <- Resource.pure(SchemaKeeperRouter.build(schemakeeperApi, swaggerApi, common.cfg))
  } yield server

  private def migrate[F[_]: Sync](configuration: Configuration): F[Unit] = for {
    flyway <- FlywayMigrationTool.build(configuration).pure
    _ <- Sync[F].delay(flyway.migrate())
  } yield ()

  private def commonSettings[F[_]: Sync](): Resource[F, Common] = for {
    cfg <- Resource.liftF(Configuration.create[F])
    context <- Resource.pure(DataSource.context(cfg))
    lock <- Resource.pure(StorageLock(cfg))
    exceptionHandler <- Resource.pure(StorageExceptionHandler(cfg))
  } yield Common(cfg, context, lock, exceptionHandler)

  final case class Common(
    cfg: Configuration,
    context: DoobieContextBase[_ <: SqlIdiom, _ <: NamingStrategy],
    lock: StorageLock[ConnectionIO],
    exceptionHandler: StorageExceptionHandler
  )
}

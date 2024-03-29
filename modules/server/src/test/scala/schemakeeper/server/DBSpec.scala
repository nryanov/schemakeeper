package schemakeeper.server

import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import com.typesafe.config.Config
import org.flywaydb.core.Flyway
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.datasource.migration.FlywayMigrationTool
import schemakeeper.server.service.DBBackedService
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.storage.exception.StorageExceptionHandler
import schemakeeper.server.storage.lock.StorageLock

import scala.concurrent.ExecutionContext

trait DBSpec extends IOSpec {
  protected var finalizer: F[Unit] = _
  private var flyway: Flyway = _

  val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  implicit val cs: ContextShift[F] = IO.contextShift(ec)

  override def afterAll(): Unit =
    finalizer.unsafeRunSync()

  override def runF[A](fa: => F[Unit]): Unit = super.runF(migrate() *> fa)

  private def migrate(): F[Unit] = IO.delay(flyway.clean()) *> IO.delay(flyway.migrate()).void

  def createService(config: Config): DBBackedService[F] = createService0(config).unsafeRunSync()

  private def createService0(config: Config): F[DBBackedService[F]] = for {
    cfg <- Configuration.create[F](config)
    context = DataSource.context(cfg)
    storage = DatabaseStorage.create(context, StorageExceptionHandler(cfg))
    flyway = FlywayMigrationTool.build(cfg)
    lock = StorageLock(cfg)
    _ <- IO.delay(flyway.migrate())
    resource <- DataSource.resource[F](cfg).allocated
  } yield {
    this.finalizer = resource._2
    this.flyway = flyway
    DBBackedService.create[F](storage, resource._1, lock)
  }
}

package schemakeeper.server.service

import java.util.concurrent.Executors
import cats.syntax.apply._
import cats.effect.{ContextShift, IO}
import com.typesafe.config.Config
import org.flywaydb.core.Flyway
import org.scalatest.{Assertion, BeforeAndAfterAll}
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.datasource.migration.FlywayMigrationTool
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.{Configuration, IOSpec}

import scala.concurrent.ExecutionContext

trait DBSpec extends IOSpec with BeforeAndAfterAll {
  protected var finalizer: F[Unit] = _
  private var flyway: Flyway = _

  val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  implicit val cs: ContextShift[F] = IO.contextShift(ec)

  override protected def afterAll(): Unit =
    finalizer.unsafeRunSync()

  override def runF[A](fa: => F[Assertion]): Assertion = super.runF(migrate() *> fa)

  private def migrate(): F[Int] = IO.delay(flyway.clean()) *> IO.delay(flyway.migrate())

  def createService(config: Config): DBBackedService[F] = createService0(config).unsafeRunSync()

  private def createService0(config: Config): F[DBBackedService[F]] = for {
    cfg <- Configuration.create[F](config)
    context = DataSource.context(cfg)
    storage = DatabaseStorage.create(context)
    flyway = FlywayMigrationTool.build(cfg)
    _ <- IO.delay(flyway.migrate())
    resource <- DataSource.resource[F](cfg).allocated
  } yield {
    this.finalizer = resource._2
    this.flyway = flyway
    DBBackedService.create[F](storage, resource._1)
  }
}

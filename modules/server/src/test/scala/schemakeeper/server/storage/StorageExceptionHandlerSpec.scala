package schemakeeper.server.storage

import schemakeeper.server.storage.exception._
import schemakeeper.server.{Configuration, Storage}
import munit._

class StorageExceptionHandlerSpec extends FunSuite {
  val cfg: Storage = Storage(
    url = "url",
    driver = "",
    username = "user",
    schema = "schema"
  )

  test("return postgresql handler") {
    assert(
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:postgresql://host:port")))
        .isInstanceOf[PostgreSQLExceptionHandler]
    )
  }

  test("return mysql handler") {
    assert(
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:mysql://host:port")))
        .isInstanceOf[MySQLExceptionHandler]
    )
  }

  test("return h2 handler") {
    assert(
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:h2://host:port"))).isInstanceOf[H2ExceptionHandler]
    )
  }

  test("return mariadb handler") {
    assert(
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:mariadb://host:port")))
        .isInstanceOf[MariaDBExceptionHandler]
    )
  }

  test("return oracle handler") {
    assert(
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:oracle://host:port")))
        .isInstanceOf[OracleExceptionHandler]
    )
  }

  test("throw error") {
    try StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:unknown://host:port")))
    catch {
      case _: IllegalArgumentException => assert(cond = true)
      case e: Throwable                => failSuite(s"Unexpected error: $e")
    }
  }
}

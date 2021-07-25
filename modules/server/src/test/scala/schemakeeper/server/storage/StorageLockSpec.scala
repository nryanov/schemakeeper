package schemakeeper.server.storage

import schemakeeper.server.{Configuration, Storage}
import schemakeeper.server.storage.lock._

import munit._

class StorageLockSpec extends FunSuite {
  val cfg = Storage(
    url = "url",
    driver = "",
    username = "user",
    schema = "schema"
  )

  test("return postgresql lock") {
    assert(
      StorageLock(Configuration(cfg.copy(url = "jdbc:postgresql://host:port"))).isInstanceOf[PostgreSQLStorageLock]
    )
  }

  test("return mysql lock") {
    assert(StorageLock(Configuration(cfg.copy(url = "jdbc:mysql://host:port"))).isInstanceOf[MySQLStorageLock])
  }

  test("return h2 lock") {
    assert(StorageLock(Configuration(cfg.copy(url = "jdbc:h2://host:port"))).isInstanceOf[H2StorageLock])
  }

  test("return mariadb lock") {
    assert(StorageLock(Configuration(cfg.copy(url = "jdbc:mariadb://host:port"))).isInstanceOf[MariaDBStorageLock])
  }

  test("return oracle lock") {
    assert(StorageLock(Configuration(cfg.copy(url = "jdbc:oracle://host:port"))).isInstanceOf[OracleStorageLock])
  }

  test("throw error") {
    try {
      StorageLock(Configuration(cfg.copy(url = "jdbc:unknown://host:port")))
    } catch {
      case _: IllegalArgumentException => assert(cond = true)
      case e: Throwable                => failSuite(s"Unexpected error: $e")
    }
  }
}

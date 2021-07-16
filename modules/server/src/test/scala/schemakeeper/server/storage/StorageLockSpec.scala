package schemakeeper.server.storage

import org.junit.runner.RunWith
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.{Configuration, Storage}
import schemakeeper.server.storage.lock._

@RunWith(classOf[JUnitRunner])
class StorageLockSpec extends WordSpec with MustMatchers {
  val cfg = Storage(
    url = "url",
    driver = "",
    username = "user",
    schema = "schema"
  )

  "StorageLock" should {
    "return postgresql lock" in {
      StorageLock(Configuration(cfg.copy(url = "jdbc:postgresql://host:port"))) mustBe a[PostgreSQLStorageLock]
    }

    "return mysql lock" in {
      StorageLock(Configuration(cfg.copy(url = "jdbc:mysql://host:port"))) mustBe a[MySQLStorageLock]
    }

    "return h2 lock" in {
      StorageLock(Configuration(cfg.copy(url = "jdbc:h2://host:port"))) mustBe a[H2StorageLock]
    }

    "return mariadb lock" in {
      StorageLock(Configuration(cfg.copy(url = "jdbc:mariadb://host:port"))) mustBe a[MariaDBStorageLock]
    }

    "return oracle lock" in {
      StorageLock(Configuration(cfg.copy(url = "jdbc:oracle://host:port"))) mustBe a[OracleStorageLock]
    }

    "throw error" in {
      assertThrows[IllegalArgumentException](StorageLock(Configuration(cfg.copy(url = "jdbc:unknown://host:port"))))
    }
  }
}

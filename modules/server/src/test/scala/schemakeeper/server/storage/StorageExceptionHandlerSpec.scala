package schemakeeper.server.storage

import org.junit.runner.RunWith
import org.scalatest.{MustMatchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.storage.exception._
import schemakeeper.server.{Configuration, Storage}

@RunWith(classOf[JUnitRunner])
class StorageExceptionHandlerSpec extends WordSpec with MustMatchers {
  val cfg = Storage(
    url = "url",
    driver = "",
    username = "user",
    schema = "schema"
  )

  "StorageExceptionHandler" should {
    "return postgresql handler" in {
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:postgresql://host:port"))) mustBe a[
        PostgreSQLExceptionHandler
      ]
    }

    "return mysql handler" in {
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:mysql://host:port"))) mustBe a[MySQLExceptionHandler]
    }

    "return h2 handler" in {
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:h2://host:port"))) mustBe a[H2ExceptionHandler]
    }

    "return mariadb handler" in {
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:mariadb://host:port"))) mustBe a[
        MariaDBExceptionHandler
      ]
    }

    "return oracle handler" in {
      StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:oracle://host:port"))) mustBe a[OracleExceptionHandler]
    }

    "throw error" in {
      assertThrows[IllegalArgumentException](
        StorageExceptionHandler(Configuration(cfg.copy(url = "jdbc:unknown://host:port")))
      )
    }
  }
}

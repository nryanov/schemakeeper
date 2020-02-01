package schemakeeper.server.datasource

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

@RunWith(classOf[JUnitRunner])
class DataSourceUtilTest extends WordSpec with Matchers {
  "DataSourceUtil" should {
    "return postgresql provider" in {
      assertResult(SupportedDatabaseProvider.PostgreSQL)(
        DataSourceUtils.detectDatabaseProvider("jdbc:postgresql://host:port")
      )
    }

    "return mysql provider" in {
      assertResult(SupportedDatabaseProvider.MySQL)(DataSourceUtils.detectDatabaseProvider("jdbc:mysql://host:port"))
    }

    "return h2 provider" in {
      assertResult(SupportedDatabaseProvider.H2)(DataSourceUtils.detectDatabaseProvider("jdbc:h2://host:port"))
    }

    "return mariadb provider" in {
      assertResult(SupportedDatabaseProvider.MariaDB)(
        DataSourceUtils.detectDatabaseProvider("jdbc:mariadb://host:port")
      )
    }

    "return oracle provider" in {
      assertResult(SupportedDatabaseProvider.Oracle)(DataSourceUtils.detectDatabaseProvider("jdbc:oracle://host:port"))
    }

    "throw error" in {
      assertThrows[IllegalArgumentException](DataSourceUtils.detectDatabaseProvider("jdbc:unknown://host:port"))
    }
  }
}

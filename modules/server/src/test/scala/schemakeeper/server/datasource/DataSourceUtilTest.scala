package schemakeeper.server.datasource

import schemakeeper.server.BaseSpec
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider

class DataSourceUtilTest extends BaseSpec {
  test("return postgresql provider") {
    assertEquals(
      SupportedDatabaseProvider.PostgreSQL.asInstanceOf[SupportedDatabaseProvider],
      DataSourceUtils.detectDatabaseProvider("jdbc:postgresql://host:port")
    )
  }

  test("return mysql provider") {
    assertEquals(
      SupportedDatabaseProvider.MySQL.asInstanceOf[SupportedDatabaseProvider],
      DataSourceUtils.detectDatabaseProvider("jdbc:mysql://host:port")
    )
  }

  test("return h2 provider") {
    assertEquals(
      SupportedDatabaseProvider.H2.asInstanceOf[SupportedDatabaseProvider],
      DataSourceUtils.detectDatabaseProvider("jdbc:h2://host:port")
    )
  }

  test("return mariadb provider") {
    assertEquals(
      SupportedDatabaseProvider.MariaDB.asInstanceOf[SupportedDatabaseProvider],
      DataSourceUtils.detectDatabaseProvider("jdbc:mariadb://host:port")
    )
  }

  test("return oracle provider") {
    assertEquals(
      SupportedDatabaseProvider.Oracle.asInstanceOf[SupportedDatabaseProvider],
      DataSourceUtils.detectDatabaseProvider("jdbc:oracle://host:port")
    )
  }

  test("throw error") {
    try {
      DataSourceUtils.detectDatabaseProvider("jdbc:unknown://host:port")
    } catch {
      case _: IllegalArgumentException => assert(cond = true)
      case _: Throwable                => failSuite("Unexpected error")
    }
  }
}

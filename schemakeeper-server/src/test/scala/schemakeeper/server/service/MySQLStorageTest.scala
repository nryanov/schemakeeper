package schemakeeper.server.service

import java.sql.DriverManager
import java.util

import cats.Id
import com.dimafeng.testcontainers.{ForAllTestContainer, MySQLContainer}
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.Configuration

@RunWith(classOf[JUnitRunner])
class MySQLStorageTest extends ServiceTest with ForAllTestContainer with BeforeAndAfterEach with BeforeAndAfterAll {
  override val container: MySQLContainer = MySQLContainer(mysqlImageVersion = "mysql:latest", databaseName = "schemakeeper")

  override lazy val schemaStorage: DBBackedService[Id] = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", container.username)
    map.put("schemakeeper.storage.password", container.password)
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", container.driverClassName)
    map.put("schemakeeper.storage.url", container.jdbcUrl)

    val config: Config = ConfigFactory.parseMap(map)
    DBBackedService.apply[Id](Configuration.apply(config))
  }

  lazy val connection = {
    Class.forName(container.driverClassName)
    val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
    connection.setAutoCommit(false)
    connection
  }

  override protected def afterEach(): Unit = {
    connection.createStatement().execute("delete from schemakeeper.subject_schema")
    connection.createStatement().execute("delete from schemakeeper.schema_info")
    connection.createStatement().execute("delete from schemakeeper.subject")
    connection.commit()
  }

  override protected def afterAll(): Unit = {
    connection.close()
  }
}

package schemakeeper.server.service

import java.sql.{Connection, DriverManager}
import java.util

import cats.Id
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.MySQLContainer
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.Configuration

@RunWith(classOf[JUnitRunner])
class MySQLStorageTest extends ServiceTest with TestContainerForAll with BeforeAndAfterEach {
  override val containerDef: MySQLContainer.Def =
    MySQLContainer.Def(dockerImageName = "mysql:5.7", databaseName = "schemakeeper")
  override var schemaStorage: DBBackedService[Id] = _
  var connection: Connection = _

  override def afterContainersStart(container: MySQLContainer): Unit = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", container.username)
    map.put("schemakeeper.storage.password", container.password)
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", container.driverClassName)
    map.put("schemakeeper.storage.url", container.jdbcUrl)

    val config: Config = ConfigFactory.parseMap(map)
    DBBackedService.apply[Id](Configuration.apply(config))

    Class.forName(container.driverClassName)
    connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
    connection.setAutoCommit(false)
  }

  override protected def afterEach(): Unit = {
    connection.createStatement().execute("delete from schemakeeper.subject_schema")
    connection.createStatement().execute("delete from schemakeeper.schema_info")
    connection.createStatement().execute("delete from schemakeeper.subject")
    connection.commit()
  }

  override def beforeContainersStop(containers: MySQLContainer): Unit = connection.close()
}

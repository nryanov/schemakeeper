package schemakeeper.server.service

import java.sql.DriverManager
import java.util

import cats.Id
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.Configuration

@RunWith(classOf[JUnitRunner])
class H2StorageTest extends ServiceTest with BeforeAndAfterEach with BeforeAndAfterAll {
  lazy val schemaStorage: DBBackedService[Id] = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", "")
    map.put("schemakeeper.storage.password", "")
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", "org.h2.Driver")
    map.put("schemakeeper.storage.maxConnections", "1")
    map.put("schemakeeper.storage.url", "jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1")

    val config: Config = ConfigFactory.parseMap(map)
    DBBackedService.apply[Id](Configuration.apply(config))
  }

  lazy val connection = {
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection("jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1", "", "")
    connection.setSchema("schemakeeper")
    connection.setAutoCommit(false)
    connection
  }

  override protected def afterEach(): Unit = {
    connection.createStatement().execute("delete from subject_schema")
    connection.createStatement().execute("delete from schema_info")
    connection.createStatement().execute("delete from subject")
    connection.commit()
  }

  override protected def afterAll(): Unit = {
    connection.close()
  }
}

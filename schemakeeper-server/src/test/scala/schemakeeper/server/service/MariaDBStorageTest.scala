package schemakeeper.server.service

import java.sql.{Connection, DriverManager}
import java.util

import cats.Id
import com.dimafeng.testcontainers.{ContainerDef, JdbcDatabaseContainer, MariaDBContainer, SingleContainer}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.Configuration

@RunWith(classOf[JUnitRunner])
class MariaDBStorageTest extends ServiceTest with TestContainerForAll with BeforeAndAfterEach {
  override val containerDef: MariaDBStorageTest.MariaDBContainer.Def =
    MariaDBStorageTest.MariaDBContainer.Def(dbName = "schemakeeper")

  override var schemaStorage: DBBackedService[Id] = _
  var connection: Connection = _

  override def afterContainersStart(container: MariaDBStorageTest.MariaDBContainer): Unit = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", container.username)
    map.put("schemakeeper.storage.password", container.password)
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", container.driverClassName)
    map.put("schemakeeper.storage.url", container.jdbcUrl)

    val config: Config = ConfigFactory.parseMap(map)
    schemaStorage = DBBackedService.apply[Id](Configuration.apply(config))

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

  override def beforeContainersStop(containers: MariaDBStorageTest.MariaDBContainer): Unit = connection.close()
}

object MariaDBStorageTest {
  import org.testcontainers.containers.{MariaDBContainer => JavaMariaDBContainer}

  case class MariaDBContainer(
    dockerImageName: String = MariaDBContainer.defaultDockerImageName,
    dbName: String = MariaDBContainer.defaultDatabaseName,
    dbUsername: String = MariaDBContainer.defaultUsername,
    dbPassword: String = MariaDBContainer.defaultPassword,
    configurationOverride: Option[String] = None
  ) extends SingleContainer[JavaMariaDBContainer[_]]
      with JdbcDatabaseContainer {

    override val container: JavaMariaDBContainer[_] = {
      val c = new JavaMariaDBContainer(dockerImageName)
      c.withDatabaseName(dbName)
      c.withUsername(dbUsername)
      c.withPassword(dbPassword)
      configurationOverride.foreach(c.withConfigurationOverride)
      c
    }

    def testQueryString: String = container.getTestQueryString

  }

  object MariaDBContainer {

    val defaultDockerImageName = s"${JavaMariaDBContainer.IMAGE}:${JavaMariaDBContainer.DEFAULT_TAG}"
    val defaultDatabaseName = "test"
    val defaultUsername = "test"
    val defaultPassword = "test"

    case class Def(
      dockerImageName: String = MariaDBContainer.defaultDockerImageName,
      dbName: String = MariaDBContainer.defaultDatabaseName,
      dbUsername: String = MariaDBContainer.defaultUsername,
      dbPassword: String = MariaDBContainer.defaultPassword,
      configurationOverride: Option[String] = None
    ) extends ContainerDef {

      override type Container = MariaDBContainer

      override def createContainer(): MariaDBContainer =
        new MariaDBContainer(
          dockerImageName,
          dbName,
          dbUsername,
          dbPassword,
          configurationOverride
        )
    }

  }

}

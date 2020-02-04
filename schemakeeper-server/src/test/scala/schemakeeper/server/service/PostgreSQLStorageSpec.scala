package schemakeeper.server.service

import java.sql.{Connection, DriverManager}
import java.util

import cats.Id
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.Configuration

@RunWith(classOf[JUnitRunner])
class PostgreSQLStorageSpec extends ServiceSpec with TestContainerForAll with BeforeAndAfterEach {
  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def(dockerImageName = "postgres:9.6")
  override var schemaStorage: DBBackedService[F] = _
  var connection: Connection = _

  override def afterContainersStart(container: PostgreSQLContainer): Unit = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", container.username)
    map.put("schemakeeper.storage.password", container.password)
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", container.driverClassName)
    map.put("schemakeeper.storage.url", container.jdbcUrl)

    val config: Config = ConfigFactory.parseMap(map)
    schemaStorage = DBBackedService.apply[F](Configuration.apply(config))

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

  override def beforeContainersStop(containers: PostgreSQLContainer): Unit = connection.close()
}

package schemakeeper.server.service

import java.util

import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.server.DBSpec

@RunWith(classOf[JUnitRunner])
class PostgreSQLStorageSpec extends ServiceSpec with TestContainerForAll with DBSpec {
  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def(dockerImageName = "postgres:9.6")
  override var schemaStorage: DBBackedService[F] = _

  override def afterContainersStart(container: PostgreSQLContainer): Unit = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", container.username)
    map.put("schemakeeper.storage.password", container.password)
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", container.driverClassName)
    map.put("schemakeeper.storage.url", container.jdbcUrl)

    val config: Config = ConfigFactory.parseMap(map)
    schemaStorage = createService(config)
  }
}

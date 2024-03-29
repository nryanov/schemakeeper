package schemakeeper.server.service

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import schemakeeper.server.DBSpec

class H2StorageSpec extends ServiceSpec with DBSpec {
  var schemaStorage: DBBackedService[F] = {
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", "")
    map.put("schemakeeper.storage.password", "")
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", "org.h2.Driver")
    map.put("schemakeeper.storage.maxConnections", "1")
    map.put("schemakeeper.storage.url", "jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1")

    val config: Config = ConfigFactory.parseMap(map)
    createService(config)
  }
}

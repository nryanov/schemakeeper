package schemakeeper.server

import com.typesafe.config.Config

class Configuration(config: Config) {
  def databaseConnectionString: String = config.getString("schemakeeper.storage.url")

  def databaseDriver: String = config.getString("schemakeeper.storage.driver")

  def databaseUsername: String = config.getString("schemakeeper.storage.username")

  def databasePassword: String = if (config.hasPath("schemakeeper.storage.password")) {
    config.getString("schemakeeper.storage.password")
  } else {
    ""
  }

  def databaseMaxConnections: Int = if (config.hasPath("schemakeeper.storage.maxConnections")) {
    config.getInt("schemakeeper.storage.maxConnections")
  } else {
    Runtime.getRuntime.availableProcessors()
  }
}

object Configuration {
  def apply(config: Config): Configuration = new Configuration(config)
}
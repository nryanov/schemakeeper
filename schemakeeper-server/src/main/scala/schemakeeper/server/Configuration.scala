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

  def databaseSchema: String = config.getString("schemakeeper.storage.schema")

  def listeningPort: Int = config.getInt("schemakeeper.server.port")

  def adminPort: Int = config.getInt("schemakeeper.server.admin.port")

  def allowsOrigin: Option[String] = {
    val origin = config.getString("schemakeeper.server.cors.allowsOrigin")
    if (origin.isEmpty) {
      None
    } else {
      Some(origin)
    }
  }

  def allowsMethods: Option[Seq[String]] = {
    // to be able to use simple comma-separated values
    val methods = config.getString("schemakeeper.server.cors.allowsMethods").split(",").toSeq
    if (methods.isEmpty) {
      None
    } else {
      Some(methods)
    }
  }

  def allowsHeaders: Option[Seq[String]] = {
    // to be able to use simple comma-separated values
    val headers = config.getString("schemakeeper.server.cors.allowsHeaders").split(",").toSeq
    if (headers.isEmpty) {
      None
    } else {
      Some(headers)
    }
  }
}

object Configuration {
  def apply(config: Config): Configuration = new Configuration(config)
}
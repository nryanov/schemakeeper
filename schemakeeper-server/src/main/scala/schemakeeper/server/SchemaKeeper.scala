package schemakeeper.server

import cats.Id
import io.finch._
import com.twitter.finagle.param.Stats
import com.twitter.server.TwitterServer
import com.twitter.finagle.Http
import com.twitter.util.Await
import com.typesafe.config.ConfigFactory
import schemakeeper.server.api.SchemaKeeperApi
import io.circe.generic.auto._
import io.finch.circe._
import schemakeeper.server.api.protocol.JsonProtocol._
import schemakeeper.server.service.DBBackedService

object SchemaKeeper extends TwitterServer {
  def main() {
    val configuration = Configuration(ConfigFactory.load())
    val service = DBBackedService[Id](configuration)
    val api = SchemaKeeperApi(service)
    val bootstrap = Bootstrap
      .configure()
      .serve[Application.Json](api.subjectVersions)
      .serve[Application.Json](api.checkSubjectSchemaCompatibility :+: api.deleteSubject :+: api.deleteSubjectVersion)
      .serve[Application.Json](api.subjectSchemaByVersion)
      .serve[Application.Json](api.schema)
      .serve[Application.Json](api.subjects)
      .serve[Application.Json](api.subjectOnlySchemaByVersion)
      .serve[Application.Json](api.registerNewSubjectSchema)
      .serve[Application.Json](api.getSubjectCompatibilityConfig :+: api.updateSubjectCompatibilityConfig)
      .toService

    val server = Http.server
      .configured(Stats(statsReceiver))
      .serve(s":${configuration.listeningPort}", bootstrap)

    onExit {
      server.close()
    }

    Await.ready(adminHttpServer)
  }
}

package schemakeeper.server

import cats.effect.IO
import io.finch._
import io.finch.circe._
import com.twitter.finagle.param.Stats
import com.twitter.server.TwitterServer
import com.twitter.finagle.Http
import com.twitter.util.Await
import com.typesafe.config.ConfigFactory
import schemakeeper.server.api.{SchemaKeeperApi, UI}
import schemakeeper.server.api.protocol.JsonProtocol._
import schemakeeper.server.service.DBBackedService

import scala.concurrent.ExecutionContext

object SchemaKeeper extends TwitterServer {
  implicit val ctx = IO.contextShift(ExecutionContext.global)

  def main() {
    val configuration = Configuration(ConfigFactory.load())
    val service = DBBackedService[IO](configuration)

    val api = SchemaKeeperApi(service)
    val ui = UI()

    val bootstrap = Bootstrap
      .configure()
      .serve[Application.Json](api.subjects
      :+: api.subjectMetadata
      :+: api.subjectVersions
      :+: api.subjectSchemasMetadata
      :+: api.subjectSchemaByVersion
      :+: api.schemaById
      :+: api.deleteSubject
      :+: api.deleteSubjectSchemaByVersion
      :+: api.checkSubjectSchemaCompatibility
      :+: api.updateSubjectCompatibility
      :+: api.getSubjectCompatibility
      :+: api.registerSchema
      :+: api.registerSchemaAndSubject
      :+: api.registerSubject
      :+: api.addSchemaToSubject
      :+: api.isSubjectExist
      :+: api.getGlobalCompatibility
      :+: api.updateGlobalCompatibility
    )
      .serve[Text.Html](ui.indexHTML)
      .serve[Text.Plain](ui.index)
      .serve[Application.Javascript](ui.mainJS :+: ui.bootstrapJS :+: ui.jQueryJS :+: ui.mustacheJS :+: ui.schemakeeperApiJS)
      .serve[Text.Css](ui.bootstrapCSS)
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

package schemakeeper.server.api

import io.finch._
import cats.effect.{ContextShift, IO}
import com.twitter.finagle.http.Status
import com.twitter.io.Buf

class UI(implicit S: ContextShift[IO]) extends Endpoint.Module[IO] {
  final val indexHTML: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/index.html")

  final val mainJS: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/js/main.js")
  final val bootstrapJS: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/js/bootstrap.min.js")
  final val jQueryJS: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/js/jquery.min.js")
  final val mustacheJS: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/js/mustache.min.js")
  final val schemakeeperApiJS: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/js/schemakeeperApi.js")

  final val boostrapCSS: Endpoint[IO, Buf] = classpathAsset("/schemakeeper/css/bootstrap.min.css")

  final val index: Endpoint[IO, Unit] = get("schemakeeper") {
    Output.unit(Status.SeeOther).withHeader("Location" -> "/schemakeeper/index.html")
  }
}

object UI {
  def apply()(implicit S: ContextShift[IO]): UI = new UI()
}

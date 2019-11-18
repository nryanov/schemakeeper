package schemakeeper.server.api

import cats.effect.{ContextShift, IO}
import com.twitter.finagle.http.Status
import com.twitter.io.Buf
import io.finch.{Endpoint, Output}
import org.slf4j.LoggerFactory

class SwaggerApi(implicit S: ContextShift[IO]) extends Endpoint.Module[IO] {
  final val swaggerDocRedirect: Endpoint[IO, Unit] = get("swagger" :: "doc") {
    Output.unit(Status.SeeOther).withHeader("Location" -> "/swagger/swagger.yml")
  }

  final val swaggerDoc: Endpoint[IO, Buf] = classpathAsset("/swagger/swagger.yml")

  final val swaggerUiRedirect: Endpoint[IO, Unit] = get("swagger" :: "ui") {
    Output.unit(Status.SeeOther).withHeader("Location" -> "/swagger/ui.html")
  }

  final val swaggerUi: Endpoint[IO, Buf] = classpathAsset("/swagger/ui.html")

  final val swaggerUiBundleJs: Endpoint[IO, Buf] = classpathAsset("/META-INF/resources/webjars/swagger-ui/3.24.0/swagger-ui-bundle.js")

  final val swaggerUiCss: Endpoint[IO, Buf] = classpathAsset("/META-INF/resources/webjars/swagger-ui/3.24.0/swagger-ui.css")

  final val swaggerUiStandalonePresetJs: Endpoint[IO, Buf] = classpathAsset("/META-INF/resources/webjars/swagger-ui/3.24.0/swagger-ui-standalone-preset.js")

  final val swaggerUiIcon32: Endpoint[IO, Buf] = classpathAsset("/META-INF/resources/webjars/swagger-ui/3.24.0/favicon-32x32.png")

  final val swaggerUiIcon16: Endpoint[IO, Buf] = classpathAsset("/META-INF/resources/webjars/swagger-ui/3.24.0/favicon-16x16.png")
}

object SwaggerApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)

  def apply()(implicit S: ContextShift[IO]): SwaggerApi = new SwaggerApi()
}

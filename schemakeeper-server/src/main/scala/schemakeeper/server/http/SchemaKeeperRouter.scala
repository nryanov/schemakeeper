package schemakeeper.server.http

import cats.effect.{Async, ConcurrentEffect, Resource, Timer}
import cats.implicits._
import org.http4s.server.{Router, Server}
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.syntax.kleisli._
import schemakeeper.server.Configuration

object SchemaKeeperRouter {
  def build[F[_]: Async: ConcurrentEffect: Timer](
    api: SchemaKeeperApi[F],
    swaggerApi: SwaggerApi[F],
    configuration: Configuration
  ): Resource[F, Server[F]] =
    BlazeServerBuilder[F]
      .bindHttp(configuration.server.port, configuration.server.host)
      .withHttpApp(Router("/" -> api.route.combineK(swaggerApi.route)).orNotFound)
      .resource
}

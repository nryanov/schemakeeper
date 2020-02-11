package schemakeeper.server.http

import cats.effect.{Async, ConcurrentEffect, Timer}
import cats.implicits._

import org.http4s.server.middleware._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.syntax.kleisli._
import schemakeeper.server.{Configuration, Cors}

object SchemaKeeperRouter {
  def build[F[_]: Async: ConcurrentEffect: Timer](
    api: SchemaKeeperApi[F],
    swaggerApi: SwaggerApi[F],
    configuration: Configuration
  ): BlazeServerBuilder[F] =
    BlazeServerBuilder[F]
      .bindHttp(configuration.server.port, configuration.server.host)
      .withHttpApp(Router("/" -> service(api, swaggerApi, configuration)).orNotFound)

  private def service[F[_]: Async: ConcurrentEffect: Timer](
    api: SchemaKeeperApi[F],
    swaggerApi: SwaggerApi[F],
    configuration: Configuration
  ) =
    configuration.server.cors
      .map(cors => CORS(api.route.combineK(swaggerApi.route), corsConfig(cors)))
      .getOrElse(api.route.combineK(swaggerApi.route))

  private def corsConfig(cors: Cors): CORSConfig =
    CORSConfig(
      anyOrigin = cors.anyOrigin,
      anyMethod = cors.anyMethod,
      allowedMethods = cors.allowsMethods.map(_.toSet),
      allowCredentials = cors.allowedCredentials,
      maxAge = cors.maxAge,
      allowedOrigins = cors.allowsOrigin.map(origin => (str: String) => str == origin).getOrElse(_ => false),
      allowedHeaders = cors.allowsHeaders.map(_.toSet)
    )
}

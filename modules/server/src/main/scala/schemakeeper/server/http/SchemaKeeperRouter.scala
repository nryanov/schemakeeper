package schemakeeper.server.http

import cats.effect.{Async, ConcurrentEffect, Timer}
import cats.implicits._
import org.http4s.Method
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.middleware._
import org.http4s.server.Router
import org.http4s.syntax.kleisli._
import schemakeeper.server.{Configuration, Cors}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

object SchemaKeeperRouter {
  def build[F[_]: Async: ConcurrentEffect: Timer](
    api: SchemaKeeperApi[F],
    swaggerApi: SwaggerApi[F],
    configuration: Configuration
  ): BlazeServerBuilder[F] =
    BlazeServerBuilder
      .apply[F](ExecutionContext.global)
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

  private def corsConfig(cors: Cors): CORSConfig = {
    val allowedOrigins = cors.allowsOrigins.map(_.split(",").toSet)

    val anyOrigin = cors.anyOrigin || allowedOrigins.fold(false)(_.contains("*"))

    CORSConfig.default
      .withAnyOrigin(anyOrigin)
      .withAnyMethod(cors.anyMethod)
      .withAllowedMethods(
        cors.allowsMethods.map(
          _.split(",")
            .map(method =>
              Method
                .fromString(method.toUpperCase)
                .getOrElse(throw new IllegalArgumentException(s"Unknown allowed method: $method"))
            )
            .toSet
        )
      )
      .withAllowCredentials(cors.allowedCredentials)
      .withMaxAge(cors.maxAge.seconds)
      .withAllowedOrigins(allowedOrigins.map(origins => (str: String) => origins.contains(str)).getOrElse(_ => false))
      .withAllowedHeaders(cors.allowsHeaders.map(_.split(",").toSet))
      .withExposedHeaders(cors.exposedHeaders.map(_.split(",").toSet))
  }
}

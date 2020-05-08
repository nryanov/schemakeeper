package schemakeeper.server

import cats.effect.Sync
import cats.syntax.functor._
import com.typesafe.config.Config
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import scala.concurrent.duration._

final case class Storage(
  url: String,
  driver: String,
  username: String,
  password: String = "",
  maxConnections: Int = Runtime.getRuntime.availableProcessors(),
  schema: String
)

final case class Cors(
  allowedCredentials: Boolean = true,
  anyOrigin: Boolean = false,
  anyMethod: Boolean = false,
  maxAge: Long = 1.day.toSeconds,
  allowsOrigins: Option[String] = None,
  allowsMethods: Option[String] = None,
  allowsHeaders: Option[String] = None,
  exposedHeaders: Option[String] = None
)

final case class Server(port: Int = 9090, host: String = "0.0.0.0", cors: Option[Cors] = None)

final case class Configuration(storage: Storage, server: Server = Server())

object Configuration {
  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def create[F[_]](implicit F: Sync[F]): F[Configuration] =
    F.delay(ConfigSource.default.at("schemakeeper").loadOrThrow[Configuration])

  def create[F[_]](value: String)(implicit F: Sync[F]): F[Configuration] =
    F.delay(ConfigSource.string(value).at("schemakeeper").loadOrThrow[Configuration])

  def create[F[_]](value: Config)(implicit F: Sync[F]): F[Configuration] =
    F.delay(ConfigSource.fromConfig(value).at("schemakeeper").loadOrThrow[Configuration])
}

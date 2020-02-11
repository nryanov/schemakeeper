package schemakeeper.server

import cats.effect.Sync
import cats.syntax.functor._
import com.typesafe.config.Config
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

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
  maxAge: Long = -1,
  allowsOrigin: Option[String] = None,
  allowsMethods: Option[Seq[String]] = None,
  allowsHeaders: Option[Seq[String]] = None,
  exposedHeaders: Option[Seq[String]] = None
)

final case class Server(port: Int = 9090, host: String = "localhost", cors: Option[Cors] = None)

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

package schemakeeper.server

import cats.effect.Sync
import cats.syntax.functor._
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

final case class Storage(
  url: String,
  driver: String,
  username: String,
  password: Option[String] = None,
  maxConnections: Int = Runtime.getRuntime.availableProcessors(),
  schema: String
)

final case class Cors(
  allowsOrigin: Option[String] = None,
  allowsMethods: Option[Seq[String]] = None,
  allowsHeaders: Option[Seq[String]] = None
)

final case class Server(port: Int = 9090, cors: Option[Cors] = None)

final case class Schemakeeper(storage: Storage, server: Server)

case class Configuration(schemakeeper: Schemakeeper)

object Configuration {
  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def create[F[_]](implicit F: Sync[F]): F[Schemakeeper] =
    F.delay(ConfigSource.default.loadOrThrow[Configuration]).map(_.schemakeeper)

  def create[F[_]](value: String)(implicit F: Sync[F]): F[Schemakeeper] =
    F.delay(ConfigSource.string(value).loadOrThrow[Configuration]).map(_.schemakeeper)
}

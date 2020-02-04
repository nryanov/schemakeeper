package schemakeeper.server.service

import cats.free.Free
import doobie.ConnectionIO
import doobie.free.connection
import doobie.implicits._
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import schemakeeper.api.{SchemaId, SchemaMetadata, SubjectMetadata, SubjectSchemaMetadata}
import schemakeeper.schema.{AvroSchemaCompatibility, AvroSchemaUtils, CompatibilityType, SchemaType}
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.datasource.migration.FlywayMigrationTool
import schemakeeper.server.storage.exception.StorageExceptionHandler

import scala.collection.JavaConverters._
import DBBackedService._
import cats.Monad
import cats.effect.Sync
import cats.implicits._
import io.chrisdavenport.log4cats.{Logger, SelfAwareStructuredLogger}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import schemakeeper.server.util.Utils

import scala.util.Try

class DBBackedService[F[_]: Sync](config: Configuration) extends Service[F] {
  implicit def unsafeLogger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private type Result[A] = Either[SchemaKeeperError, A]
  private val datasource = DataSource.build(config)
  private val storage = DatabaseStorage(DataSource.context(config))
  private val exceptionHandler = StorageExceptionHandler(config)

  override def subjects(): F[Result[List[String]]] = for {
    _ <- Logger[F].info("Get subjects list")
    result <- transaction {
      storage.subjects()
    }.map {
      case Left(e)  => Left(BackendError(e))
      case Right(l) => Right(l)
    }
  } yield result

  override def subjectMetadata(subject: String): F[Result[SubjectMetadata]] = for {
    _ <- Logger[F].info(s"Get subject metadata: $subject")
    result <- transaction {
      storage.subjectMetadata(subject)
    }.map {
      case Left(e)           => Left(BackendError(e))
      case Right(None)       => Left(SubjectDoesNotExist(subject))
      case Right(Some(meta)) => Right(meta)
    }
  } yield result

  //todo: we need to restrict to change subject compatibility freely from one type to another
  override def updateSubjectSettings(
    subject: String,
    compatibilityType: CompatibilityType,
    isLocked: Boolean
  ): F[Result[SubjectMetadata]] = for {
    _ <- Logger[F].info(s"Update subject settings: $subject -> (${compatibilityType.identifier}, $isLocked)")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[SubjectMetadata]](storage.isSubjectExist(subject))(
        storage.updateSubjectSettings(subject, compatibilityType, isLocked).map(Right(_)),
        pure(Left(SubjectDoesNotExist(subject)))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(r) =>
        r match {
          case Left(e)  => Left(e)
          case Right(v) => Right(v)
        }
    }
  } yield result

  override def subjectVersions(subject: String): F[Result[List[Int]]] = for {
    _ <- Logger[F].info(s"Get subject version list")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[List[Int]]](storage.isSubjectExist(subject))(
        storage.subjectVersions(subject).map(Right(_)),
        pure(Left(SubjectDoesNotExist(subject)))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(r) =>
        r match {
          case Left(e)  => Left(e)
          case Right(v) => Right(v)
        }
    }
  } yield result

  override def subjectSchemasMetadata(subject: String): F[Result[List[SubjectSchemaMetadata]]] = for {
    _ <- Logger[F].info(s"Get subject schemas metadata list")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[List[SubjectSchemaMetadata]]](storage.isSubjectExist(subject))(
        storage.subjectSchemasMetadata(subject).map(Right(_)),
        pure(Left(SubjectDoesNotExist(subject)))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(r) =>
        r match {
          case Left(e)  => Left(e)
          case Right(v) => Right(v)
        }
    }
  } yield result

  override def subjectSchemaByVersion(subject: String, version: Int): F[Result[SubjectSchemaMetadata]] = for {
    _ <- Logger[F].info(s"Get subject schema: $subject by version: $version")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[Option[SubjectSchemaMetadata]]](storage.isSubjectExist(subject))(
        storage.subjectSchemaByVersion(subject, version).map(Right(_)),
        pure(Left(SubjectDoesNotExist(subject)))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(result) =>
        result match {
          case Left(e)        => Left(e)
          case Right(None)    => Left(SubjectSchemaVersionDoesNotExist(subject, version))
          case Right(Some(v)) => Right(v)
        }
    }
  } yield result

  override def schemaById(id: Int): F[Result[SchemaMetadata]] = for {
    _ <- Logger[F].info(s"Get schema by id: $id")
    result <- transaction {
      storage.schemaById(id)
    }.map {
      case Left(e)             => Left(BackendError(e))
      case Right(None)         => Left(SchemaIdDoesNotExist(id))
      case Right(Some(schema)) => Right(schema)
    }
  } yield result

  override def schemaIdBySubjectAndSchema(subject: String, schema: String): F[Either[SchemaKeeperError, SchemaId]] =
    for {
      _ <- Logger[F].info(s"Get schema id: $subject - $schema")
      result <- validateSchema(schema).flatMap {
        case Left(e) => Monad[F].pure[Result[SchemaId]](Left(e))
        case Right(_) =>
          transaction {
            storage.schemaByHash(Utils.toMD5Hex(schema)).flatMap[Result[SchemaId]] {
              case None => pure(Left(SchemaIsNotRegistered(schema)))
              case Some(meta) =>
                Monad[ConnectionIO].ifM(storage.isSubjectConnectedToSchema(subject, meta.getSchemaId))(
                  pure(Right(SchemaId.instance(meta.getSchemaId))),
                  pure(Left(SubjectIsNotConnectedToSchema(subject, meta.getSchemaId)))
                )
            }
          }.map {
            case Left(e) => Left(BackendError(e))
            case Right(tx) =>
              tx match {
                case Left(e)  => Left(e)
                case Right(v) => Right(v)
              }
          }
      }
    } yield result

  override def deleteSubject(subject: String): F[Result[Boolean]] = for {
    _ <- Logger[F].info(s"Delete subject: $subject")
    result <- transaction {
      storage.deleteSubject(subject)
    }.map {
      case Left(e)  => Left(BackendError(e))
      case Right(v) => Right(v)
    }
  } yield result

  override def deleteSubjectSchemaByVersion(subject: String, version: Int): F[Result[Boolean]] = for {
    _ <- Logger[F].info(s"Delete subject schema by version: $subject - $version")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[Boolean]](storage.isSubjectExist(subject))(
        storage.deleteSubjectSchemaByVersion(subject, version).map(Right(_)),
        pure(Left(SubjectDoesNotExist(subject)))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) =>
        tx match {
          case Left(e)        => Left(e)
          case Right(v) if v  => Right(v)
          case Right(v) if !v => Left(SubjectSchemaVersionDoesNotExist(subject, version))
        }
    }
  } yield result

  override def checkSubjectSchemaCompatibility(subject: String, schema: String): F[Result[Boolean]] = for {
    _ <- Logger[F].info(s"Check subject schema compatibility: $subject - $schema")
    result <- validateSchema(schema).flatMap {
      case Left(e) => Monad[F].pure[Result[Boolean]](Left(e))
      case Right(newSchema) =>
        transaction {
          storage.getSubjectCompatibility(subject).flatMap[Result[Boolean]] {
            case Some(compatibilityType) =>
              isSchemaCompatible(subject, newSchema, compatibilityType).map(Right(_))
            case None => pure[Result[Boolean]](Left(SubjectDoesNotExist(subject)))
          }
        }.map {
          case Left(e) => Left(BackendError(e))
          case Right(tx) =>
            tx match {
              case Left(e)  => Left(e)
              case Right(v) => Right(v)
            }
        }
    }
  } yield result

  override def getSubjectSchemas(subject: String): F[Result[List[SchemaMetadata]]] = for {
    _ <- Logger[F].info(s"Get last subject schemas: $subject")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[List[SchemaMetadata]]](storage.isSubjectExist(subject))(
        storage.getSubjectSchemas(subject).map(Right(_)),
        pure(Left(SubjectDoesNotExist(subject)))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) =>
        tx match {
          case Left(e)               => Left(e)
          case Right(l) if l.isEmpty => Left(SubjectHasNoRegisteredSchemas(subject))
          case Right(l)              => Right(l)
        }
    }
  } yield result

  override def registerSchema(schema: String, schemaType: SchemaType): F[Result[SchemaId]] = for {
    _ <- Logger[F].info(s"Register new schema: $schema - ${schemaType.identifier}")
    result <- validateSchema(schema).flatMap {
      case Left(e) => Monad[F].pure[Result[SchemaId]](Left(e))
      case Right(_) =>
        transaction {
          val schemaHash = Utils.toMD5Hex(schema)
          storage.schemaByHash(schemaHash).flatMap[Result[SchemaId]] {
            case Some(schemaText) => pure[Result[SchemaId]](Left(SchemaIsAlreadyExist(schemaText.getSchemaId, schema)))
            case None             => storage.registerSchema(schema, schemaHash, schemaType).map(SchemaId.instance).map(Right(_))
          }
        }.map {
          case Left(e) => Left(BackendError(e))
          case Right(tx) =>
            tx match {
              case Left(e)      => Left(e)
              case Right(value) => Right(value)
            }
        }
    }
  } yield result

  override def registerSchema(
    subject: String,
    schemaText: String,
    compatibilityType: CompatibilityType,
    schemaType: SchemaType
  ): F[Result[SchemaId]] = for {
    _ <- Logger[F].info(s"Register schema: $schemaText and add to subject: $subject")
    result <- validateSchema(schemaText).flatMap[Result[SchemaId]] {
      case Left(e) => Monad[F].pure(Left(e))
      case Right(schema) =>
        transaction {
          val schemaHash = Utils.toMD5Hex(schemaText)
          storage
            .schemaByHash(schemaHash)
            .flatMap {
              case None       => storage.registerSchema(schemaText, schemaHash, schemaType)
              case Some(meta) => pure[Int](meta.getSchemaId)
            }
            .flatMap(schemaId =>
              storage.subjectMetadata(subject).flatMap {
                case None =>
                  storage.registerSubject(subject, compatibilityType, isLocked = false).map(meta => (schemaId, meta))
                case Some(meta) => pure[(Int, SubjectMetadata)]((schemaId, meta))
              }
            )
            .flatMap[Result[SchemaId]] {
              case (schemaId, subjectMeta) =>
                if (subjectMeta.isLocked) {
                  pure(Left(SubjectIsLocked(subject)))
                } else {
                  Monad[ConnectionIO].ifM[Result[SchemaId]](
                    isSchemaCompatible(subject, schema, subjectMeta.getCompatibilityType)
                  )(
                    Monad[ConnectionIO].ifM(storage.isSubjectConnectedToSchema(subject, schemaId))(
                      pure(Left(SubjectIsAlreadyConnectedToSchema(subject, schemaId))),
                      storage
                        .getNextVersionNumber(subject)
                        .flatMap(version =>
                          storage
                            .addSchemaToSubject(subject, schemaId, version)
                            .map(_ => Right(SchemaId.instance(schemaId)))
                        )
                    ),
                    pure(Left(SchemaIsNotCompatible(subject, schemaText, subjectMeta.getCompatibilityType)))
                  )
                }
            }
        }.flatMap {
          case Left(e) =>
            if (exceptionHandler.isRecoverable(e)) {
              registerSchema(subject, schemaText, compatibilityType, schemaType)
            } else {
              Monad[F].pure(Left(BackendError(e)))
            }
          case Right(v) => Monad[F].pure(v)
        }
    }
  } yield result

  override def registerSubject(
    subject: String,
    compatibilityType: CompatibilityType,
    isLocked: Boolean
  ): F[Result[SubjectMetadata]] = for {
    _ <- Logger[F].info(s"Register new subject: $subject, ${compatibilityType.identifier}")
    result <- transaction {
      Monad[ConnectionIO].ifM[Result[SubjectMetadata]](storage.isSubjectExist(subject))(
        pure(Left(SubjectIsAlreadyExists(subject))),
        storage.registerSubject(subject, compatibilityType, isLocked).map(Right(_))
      )
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) =>
        tx match {
          case Left(e)  => Left(e)
          case Right(v) => Right(v)
        }
    }
  } yield result

  override def addSchemaToSubject(subject: String, schemaId: Int): F[Result[Int]] = for {
    _ <- Logger[F].info(s"Add schema: $schemaId to subject: $subject")
    result <- transaction {
      storage.subjectMetadata(subject).flatMap {
        case None => pure[Result[Int]](Left(SubjectDoesNotExist(subject)))
        case Some(meta) =>
          if (meta.isLocked) {
            pure[Result[Int]](Left(SubjectIsLocked(subject)))
          } else {
            storage.schemaById(schemaId).flatMap {
              case None => pure[Result[Int]](Left(SchemaIdDoesNotExist(schemaId)))
              case Some(schemaMetadata) =>
                Monad[ConnectionIO].ifM[Result[Int]](storage.isSubjectConnectedToSchema(subject, schemaId))(
                  pure(Left(SubjectIsAlreadyConnectedToSchema(subject, schemaId))),
                  Monad[ConnectionIO]
                    .ifM(isSchemaCompatible(subject, schemaMetadata.getSchema, meta.getCompatibilityType))(
                      storage
                        .getNextVersionNumber(subject)
                        .flatMap(version =>
                          storage.addSchemaToSubject(subject, schemaId, version).map(_ => Right(version))
                        ),
                      pure(
                        Left(SchemaIsNotCompatible(subject, schemaMetadata.getSchemaText, meta.getCompatibilityType))
                      )
                    )
                )
            }
          }
      }
    }.map {
      case Left(e) =>
        if (exceptionHandler.isRecoverable(e)) {
          // it may happen if multiple transactions will try to add schema to subject.
          // in this case the constraint violation error will be thrown which is recoverable
          Left(SubjectIsAlreadyConnectedToSchema(subject, schemaId))
        } else {
          Left(BackendError(e))
        }
      case Right(tx) =>
        tx match {
          case Left(e)  => Left(e)
          case Right(v) => Right(v)
        }
    }
  } yield result

  private def transaction[A](query: ConnectionIO[A]): F[Either[Throwable, A]] = Monad[F].pure {
    datasource.use { xa =>
      query.transact(xa)
    }.attempt.unsafeRunSync()
  }

  private def validateSchema(schemaText: String): F[Result[Schema]] = Monad[F].pure {
    Try {
      AvroSchemaUtils.parseSchema(schemaText)
    }.toEither.leftMap(_ => SchemaIsNotValid(schemaText))
  }

  private def isSchemaCompatible(
    subject: String,
    newSchema: Schema,
    compatibilityType: CompatibilityType
  ): ConnectionIO[Boolean] = compatibilityType match {
    case CompatibilityType.NONE => Free.pure[connection.ConnectionOp, Boolean](true)
    case CompatibilityType.BACKWARD =>
      getLastSchemaParsed(subject).map(
        _.forall(previousSchema => AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema))
      )
    case CompatibilityType.FORWARD =>
      getLastSchemaParsed(subject).map(
        _.forall(previousSchema => AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(newSchema, previousSchema))
      )
    case CompatibilityType.FULL =>
      getLastSchemaParsed(subject).map(
        _.forall(previousSchema => AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(newSchema, previousSchema))
      )
    case CompatibilityType.BACKWARD_TRANSITIVE =>
      getLastSchemasParsed(subject).map(previousSchemas =>
        AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas.asJava)
      )
    case CompatibilityType.FORWARD_TRANSITIVE =>
      getLastSchemasParsed(subject).map(previousSchemas =>
        AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas.asJava)
      )
    case CompatibilityType.FULL_TRANSITIVE =>
      getLastSchemasParsed(subject).map(previousSchemas =>
        AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas.asJava)
      )
  }

  private def getLastSchemaParsed(subject: String): ConnectionIO[Option[Schema]] =
    storage.getLastSubjectSchema(subject).map(_.map(_.getSchema))

  private def getLastSchemasParsed(subject: String): ConnectionIO[List[Schema]] =
    storage.getSubjectSchemas(subject).map(_.map(_.getSchema))

  private def pure[A](a: A): Free[connection.ConnectionOp, A] = Free.pure[connection.ConnectionOp, A](a)
}

object DBBackedService {
  def apply[F[_]: Sync](config: Configuration): DBBackedService[F] = {
    FlywayMigrationTool.migrate(config)
    new DBBackedService[F](config)
  }
}

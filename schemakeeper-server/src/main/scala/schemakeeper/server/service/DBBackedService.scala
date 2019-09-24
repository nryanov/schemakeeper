package schemakeeper.server.service

import cats.free.Free
import doobie.ConnectionIO
import doobie.free.connection
import doobie.implicits._
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import schemakeeper.api.{SchemaId, SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{AvroSchemaCompatibility, AvroSchemaUtils, CompatibilityType, SchemaType}
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.datasource.migration.FlywayMigrationTool
import schemakeeper.server.storage.exception.StorageExceptionHandler

import scala.collection.JavaConverters._
import DBBackedService._
import cats.Monad
import cats.implicits._
import schemakeeper.server.util.Utils

import scala.util.Try

class DBBackedService[F[_] : Monad](config: Configuration) extends Service[F] {
  type Result[A] = Either[SchemaKeeperError, A]
  val datasource = DataSource.build(config)
  val storage = DatabaseStorage(DataSource.context(config))
  val exceptionHandler = StorageExceptionHandler(config)

  override def subjects(): F[Result[List[String]]] = {
    logger.info("Get subjects list")

    transaction {
      storage.subjects()
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(l) => Right(l)
    }
  }

  override def subjectMetadata(subject: String): F[Result[SubjectMetadata]] = {
    logger.info(s"Get subject metadata: $subject")

    transaction {
      storage.subjectMetadata(subject)
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(None) => Left(SubjectDoesNotExist(subject))
      case Right(Some(meta)) => Right(meta)
    }
  }

  override def subjectVersions(subject: String): F[Result[List[Int]]] = {
    logger.info(s"Get subject version list")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[List[Int]]](exist => if (exist) {
        storage.subjectVersions(subject).map(Right(_))
      } else {
        pure[Result[List[Int]]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(r) => r match {
        case Left(e) => Left(e)
        case Right(v) if v.isEmpty => Left(SubjectHasNoRegisteredSchemas(subject))
        case Right(v) => Right(v)
      }
    }
  }

  override def subjectSchemasMetadata(subject: String): F[Result[List[SchemaMetadata]]] = {
    logger.info(s"Get subject schemas metadata list")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[List[SchemaMetadata]]](exist => if (exist) {
        storage.subjectSchemasMetadata(subject).map(Right(_))
      } else {
        pure[Result[List[SchemaMetadata]]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(r) => r match {
        case Left(e) => Left(e)
        case Right(v) if v.isEmpty => Left(SubjectHasNoRegisteredSchemas(subject))
        case Right(v) => Right(v)
      }
    }
  }

  override def subjectSchemaByVersion(subject: String, version: Int): F[Result[SchemaMetadata]] = {
    logger.info(s"Get subject schema: $subject by version: $version")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Option[SchemaMetadata]]](exist => if (exist) {
        storage.subjectSchemaByVersion(subject, version).map(Right(_))
      } else {
        pure[Result[Option[SchemaMetadata]]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(result) => result match {
        case Left(e) => Left(e)
        case Right(None) => Left(SubjectSchemaVersionDoesNotExist(subject, version))
        case Right(Some(v)) => Right(v)
      }
    }
  }

  override def schemaById(id: Int): F[Result[SchemaMetadata]] = {
    logger.info(s"Get schema by id: $id")

    transaction {
      storage.schemaById(id)
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(None) => Left(SchemaDoesNotExist(id))
      case Right(Some(schema)) => Right(schema)
    }
  }

  override def deleteSubject(subject: String): F[Result[Boolean]] = {
    logger.info(s"Delete subject: $subject")

    transaction {
      storage.deleteSubject(subject)
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(v) => Right(v)
    }
  }

  override def deleteSubjectSchemaByVersion(subject: String, version: Int): F[Result[Boolean]] = {
    logger.info(s"Delete subject schema by version: $subject - $version")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Boolean]](isExists => if (isExists) {
        storage.deleteSubjectSchemaByVersion(subject, version).map(Right(_))
      } else {
        pure[Result[Boolean]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(v) if v => Right(v)
        case Right(v) if !v => Left(SubjectSchemaVersionDoesNotExist(subject, version))
      }
    }
  }

  override def checkSubjectSchemaCompatibility(subject: String, schema: String): F[Result[Boolean]] = {
    logger.info(s"Check subject schema compatibility: $subject - $schema")

    validateSchema(schema).flatMap {
      case Left(e) => Monad[F].pure[Result[Boolean]](Left(e))
      case Right(newSchema) => transaction {
        storage.isSubjectExist(subject).flatMap[Result[Boolean]](isExists => if (isExists) {
          storage.getSubjectCompatibility(subject).flatMap {
            case Some(compatibilityType) =>
              isSchemaCompatible(subject, newSchema, compatibilityType).map(Right(_))
            case None => pure[Result[Boolean]](Left(SubjectDoesNotExist(subject))) // should never happen
          }
        } else {
          pure[Result[Boolean]](Left(SubjectDoesNotExist(subject)))
        })
      }.map {
        case Left(e) => Left(BackendError(e))
        case Right(tx) => tx match {
          case Left(e) => Left(e)
          case Right(v) => Right(v)
        }
      }
    }
  }

  override def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Result[Boolean]] = {
    logger.info(s"Update subject: $subject compatibility type: ${compatibilityType.identifier}")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Boolean]](isExists => if (isExists) {
        storage.updateSubjectCompatibility(subject, compatibilityType).map(Right(_))
      } else {
        pure[Result[Boolean]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(v) => Right(v) //todo: we need to restrict to change subject compatibility freely from one type to another
      }
    }
  }

  override def getSubjectCompatibility(subject: String): F[Result[CompatibilityType]] = {
    logger.info(s"Get subject compatibility type: $subject")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Option[CompatibilityType]]](isExists => if (isExists) {
        storage.getSubjectCompatibility(subject).map(Right(_))
      } else {
        pure[Result[Option[CompatibilityType]]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(None) => Left(SubjectDoesNotExist(subject)) // should never happen due to we check subject existence before getting compatibilityType
        case Right(Some(v)) => Right(v)
      }
    }
  }

  override def getLastSubjectSchema(subject: String): F[Result[SchemaMetadata]] = {
    logger.info(s"Get last subject schema: $subject")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Option[SchemaMetadata]]](isExists => if (isExists) {
        storage.getLastSubjectSchema(subject).map(Right(_))
      } else {
        pure[Result[Option[SchemaMetadata]]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(None) => Left(SubjectHasNoRegisteredSchemas(subject))
        case Right(Some(schema)) => Right(schema)
      }
    }
  }

  override def getSubjectSchemas(subject: String): F[Result[List[SchemaMetadata]]] = {
    logger.info(s"Get last subject schemas: $subject")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[List[SchemaMetadata]]](isExists => if (isExists) {
        storage.getSubjectSchemas(subject).map(Right(_))
      } else {
        pure[Result[List[SchemaMetadata]]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(l) if l.isEmpty => Left(SubjectHasNoRegisteredSchemas(subject))
        case Right(l) => Right(l)
      }
    }
  }

  override def registerSchema(schema: String, schemaType: SchemaType): F[Result[SchemaId]] = {
    logger.info(s"Register new schema: $schema - ${schemaType.identifier}")

    validateSchema(schema).flatMap {
      case Left(e) => Monad[F].pure[Result[SchemaId]](Left(e))
      case Right(_) => transaction {
        val schemaHash = Utils.toMD5Hex(schema)
        storage.schemaByHash(schemaHash).flatMap[Result[SchemaId]] {
          case Some(schemaText) => pure[Result[SchemaId]](Left(SchemaIsAlreadyExist(schemaText.getSchemaId, schema)))
          case None => storage.registerSchema(schema, schemaHash, schemaType).map(SchemaId.instance).map(Right(_))
        }
      }.map {
        case Left(e) => Left(BackendError(e))
        case Right(tx) => tx match {
          case Left(e) => Left(e)
          case Right(value) => Right(value)
        }
      }
    }
  }

  override def registerSchema(subject: String, schemaText: String, compatibilityType: CompatibilityType, schemaType: SchemaType): F[Result[SchemaId]] = {
    logger.info(s"Register schema: $schemaText and add to subject: $subject")

    validateSchema(schemaText).flatMap {
      case Left(e) => Monad[F].pure[Result[SchemaId]](Left(e))
      case Right(schema) => transaction {
        storage.subjectMetadata(subject).flatMap {
          case None => storage.registerSubject(subject, compatibilityType)
          case Some(_) => pure[Unit](())
        }.flatMap(_ => {
          val schemaHash = Utils.toMD5Hex(schemaText)
          storage.schemaByHash(schemaHash).flatMap {
            case None => storage.registerSchema(schemaText, schemaHash, schemaType)
            case Some(meta) => pure[Int](meta.getSchemaId)
          }.flatMap(schemaId => {
            storage.getSubjectCompatibility(subject).flatMap {
              case None => pure[Result[SchemaId]](Left(BackendError(new Exception("Something bad happened")))) // should never happen
              case Some(subjectCurrentCompatibilityType) => isSchemaCompatible(subject, schema, subjectCurrentCompatibilityType).flatMap(isCompatible => if (isCompatible) {
                storage.isSubjectConnectedToSchema(subject, schemaId).flatMap(isConnected => if (isConnected) {
                  pure[Result[SchemaId]](Left(SubjectIsAlreadyConnectedToSchema(subject, schemaId)))
                } else {
                  storage.getNextVersionNumber(subject)
                    .flatMap[Result[SchemaId]](version => storage.addSchemaToSubject(subject, schemaId, version).map(_ => Right(SchemaId.instance(schemaId))))
                })
              } else {
                pure[Result[SchemaId]](Left(SchemaIsNotCompatible(subject, schemaText, subjectCurrentCompatibilityType)))
              })
            }
          })
        })
      }.flatMap {
        case Left(e) => if(exceptionHandler.isRecoverable(e)) {
          registerSchema(subject, schemaText, compatibilityType, schemaType)
        } else {
          Monad[F].pure[Result[SchemaId]](Left(BackendError(e)))
        }
        case Right(v) => Monad[F].pure[Result[SchemaId]](v)
      }
    }
  }

  override def registerSubject(subject: String, compatibilityType: CompatibilityType): F[Result[Unit]] = {
    logger.info(s"Register new subject: $subject, ${compatibilityType.identifier}")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Unit]](isExists => if (isExists) {
        pure[Result[Unit]](Left(SubjectIsAlreadyExists(subject)))
      } else {
        storage.registerSubject(subject, compatibilityType).map(Right(_))
      })
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(v) => Right(v)
      }
    }
  }

  override def addSchemaToSubject(subject: String, schemaId: Int): F[Result[Int]] = {
    logger.info(s"Add schema: $schemaId to subject: $subject")

    transaction {
      storage.isSubjectExist(subject).flatMap[Result[Int]](isExists => if (isExists) {
        storage.schemaById(schemaId).flatMap {
          case None => pure[Result[Int]](Left(SchemaDoesNotExist(schemaId)))
          case Some(_) => storage.isSubjectConnectedToSchema(subject, schemaId).flatMap(isConnected => if (isConnected) {
            pure[Result[Int]](Left(SubjectIsAlreadyConnectedToSchema(subject, schemaId)))
          } else {
            storage.getNextVersionNumber(subject).flatMap(version => storage.addSchemaToSubject(subject, schemaId, version).map(_ => Right(version)))
          })
        }
      } else {
        pure[Result[Int]](Left(SubjectDoesNotExist(subject)))
      })
    }.map {
      case Left(e) => if (exceptionHandler.isRecoverable(e)) {
        // it may happen if multiple transactions will try to add schema to subject.
        // in this case the constraint violation error will be thrown which is recoverable
        Left(SubjectIsAlreadyConnectedToSchema(subject, schemaId))
      } else {
        Left(BackendError(e))
      }
      case Right(tx) => tx match {
        case Left(e) => Left(e)
        case Right(v) => Right(v)
      }
    }
  }

  override def isSubjectExist(subject: String): F[Result[Boolean]] = {
    logger.info(s"Check if subject: $subject exists")

    transaction {
      storage.isSubjectExist(subject)
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(v) => Right(v)
    }
  }

  override def getGlobalCompatibility(): F[Result[CompatibilityType]] = {
    logger.info("Get global compatibility type")

    transaction {
      storage.getGlobalCompatibility()
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(None) => Left(ConfigIsNotDefined("default.compatibility"))
      case Right(Some(v)) => Right(v)
    }
  }

  override def updateGlobalCompatibility(compatibilityType: CompatibilityType): F[Result[Boolean]] = {
    logger.info(s"Update global compatibility type: ${compatibilityType.identifier}")

    transaction {
      storage.updateGlobalCompatibility(compatibilityType)
    }.map {
      case Left(e) => Left(BackendError(e))
      case Right(v) => Right(v)
    }
  }

  private def transaction[A](query: ConnectionIO[A]): F[Either[Throwable, A]] = Monad[F].pure {
    datasource.use {
      xa => query.transact(xa)
    }.attempt.unsafeRunSync()
  }

  private def validateSchema(schemaText: String): F[Result[Schema]] = Monad[F].pure {
    Try {
      AvroSchemaUtils.parseSchema(schemaText)
    }.toEither.leftMap(_ => SchemaIsNotValid(schemaText))
  }

  private def isSchemaCompatible(subject: String, newSchema: Schema, compatibilityType: CompatibilityType): ConnectionIO[Boolean] = compatibilityType match {
    case CompatibilityType.NONE => Free.pure[connection.ConnectionOp, Boolean](true)
    case CompatibilityType.BACKWARD => getLastSchemaParsed(subject).map(_.forall(previousSchema => AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema)))
    case CompatibilityType.FORWARD => getLastSchemaParsed(subject).map(_.forall(previousSchema => AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(newSchema, previousSchema)))
    case CompatibilityType.FULL => getLastSchemaParsed(subject).map(_.forall(previousSchema => AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(newSchema, previousSchema)))
    case CompatibilityType.BACKWARD_TRANSITIVE => getLastSchemasParsed(subject).map(previousSchemas => AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas.asJava))
    case CompatibilityType.FORWARD_TRANSITIVE => getLastSchemasParsed(subject).map(previousSchemas => AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas.asJava))
    case CompatibilityType.FULL_TRANSITIVE => getLastSchemasParsed(subject).map(previousSchemas => AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas.asJava))
  }

  private def getLastSchemaParsed(subject: String): ConnectionIO[Option[Schema]] = storage.getLastSubjectSchema(subject).map(_.map(_.getSchema))

  private def getLastSchemasParsed(subject: String): ConnectionIO[List[Schema]] = storage.getSubjectSchemas(subject).map(_.map(_.getSchema))

  private def pure[A](a: A): Free[connection.ConnectionOp, A] = Free.pure[connection.ConnectionOp, A](a)
}

object DBBackedService {
  private val logger = LoggerFactory.getLogger(DBBackedService.getClass)

  def apply[F[_] : Monad](config: Configuration): DBBackedService[F] = {
    FlywayMigrationTool.migrate(config)
    new DBBackedService[F](config)
  }
}

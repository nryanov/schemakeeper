package schemakeeper.server.service

import doobie.ConnectionIO
import doobie.free.connection
import doobie.implicits._
import org.apache.avro.Schema
import cats.~>
import cats.free.Free
import cats.effect.Sync
import cats.syntax.apply._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import cats.syntax.option._
import cats.syntax.monadError._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import schemakeeper.server.util.Utils
import schemakeeper.server.storage.SchemaStorage
import schemakeeper.api.{SchemaId, SchemaMetadata, SubjectMetadata, SubjectSchemaMetadata}
import schemakeeper.schema.{AvroSchemaCompatibility, AvroSchemaUtils, CompatibilityType, SchemaType}
import schemakeeper.server.SchemaKeeperError._
import schemakeeper.server.storage.lock.StorageLock

import scala.collection.JavaConverters._

class DBBackedService[F[_]](
  storage: SchemaStorage[ConnectionIO],
  transact: ConnectionIO ~> F,
  storageLock: StorageLock[ConnectionIO]
)(implicit F: Sync[F])
    extends Service[F] {
  implicit def unsafeLogger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def subjects(): F[List[String]] = for {
    _ <- Logger[F].info("Get subjects list")
    result <- transact(storage.subjects())
  } yield result

  override def subjectMetadata(subject: String): F[SubjectMetadata] = for {
    _ <- Logger[F].info(s"Get subject metadata: $subject")
    subjectMetadata <- transact(storage.subjectMetadata(subject))
    result <- subjectMetadata.liftTo[F](SubjectDoesNotExist(subject))
  } yield result

  // todo: we need to restrict to change subject compatibility freely from one type to another
  override def updateSubjectSettings(
    subject: String,
    compatibilityType: CompatibilityType,
    isLocked: Boolean
  ): F[SubjectMetadata] = for {
    _ <- Logger[F].info(s"Update subject settings: $subject -> (${compatibilityType.identifier}, $isLocked)")
    result <- transact(isSubjectExists(subject) *> storage.updateSubjectSettings(subject, compatibilityType, isLocked))
  } yield result

  override def subjectVersions(subject: String): F[List[Int]] = for {
    _ <- Logger[F].info(s"Get subject version list")
    result <- transact(isSubjectExists(subject) *> storage.subjectVersions(subject))
  } yield result

  override def subjectSchemasMetadata(subject: String): F[List[SubjectSchemaMetadata]] = for {
    _ <- Logger[F].info(s"Get subject schemas metadata list")
    result <- transact(isSubjectExists(subject) *> storage.subjectSchemasMetadata(subject))
      .ensure(SubjectHasNoRegisteredSchemas(subject))(_.nonEmpty)
  } yield result

  override def subjectSchemaByVersion(subject: String, version: Int): F[SubjectSchemaMetadata] = for {
    _ <- Logger[F].info(s"Get subject schema: $subject by version: $version")
    optional <- transact(isSubjectExists(subject) *> storage.subjectSchemaByVersion(subject, version))
    result <- optional.liftTo[F](SubjectSchemaVersionDoesNotExist(subject, version))
  } yield result

  override def schemaById(id: Int): F[SchemaMetadata] = for {
    _ <- Logger[F].info(s"Get schema by id: $id")
    optional <- transact(storage.schemaById(id))
    result <- optional.liftTo[F](SchemaIdDoesNotExist(id))
  } yield result

  override def schemaIdBySubjectAndSchema(subject: String, schemaText: String): F[SchemaId] =
    for {
      _ <- Logger[F].info(s"Get schema id: $subject - $schemaText")
      _ <- validateSchema(schemaText)
      result <- transact {
        for {
          meta <- storage
            .schemaByHash(Utils.toMD5Hex(schemaText))
            .ensure(SchemaIsNotRegistered(schemaText))(_.isDefined)
            .map(_.get)
          _ <- storage
            .isSubjectConnectedToSchema(subject, meta.getSchemaId)
            .ensure(SubjectIsNotConnectedToSchema(subject, meta.getSchemaId))(identity)
        } yield SchemaId.instance(meta.getSchemaId)
      }
    } yield result

  override def deleteSubject(subject: String): F[Boolean] = for {
    _ <- Logger[F].info(s"Delete subject: $subject")
    result <- transact(storage.deleteSubject(subject))
  } yield result

  override def deleteSubjectSchemaByVersion(subject: String, version: Int): F[Boolean] = for {
    _ <- Logger[F].info(s"Delete subject schema by version: $subject - $version")
    result <- transact {
      for {
        _ <- isSubjectExists(subject)
        deleted <- storage
          .deleteSubjectSchemaByVersion(subject, version)
          .ensure(SubjectSchemaVersionDoesNotExist(subject, version))(identity)
      } yield deleted
    }
  } yield result

  override def checkSubjectSchemaCompatibility(subject: String, schemaText: String): F[Boolean] = for {
    _ <- Logger[F].info(s"Check subject schema compatibility: $subject - $schemaText")
    newSchema <- validateSchema(schemaText)
    result <- transact {
      for {
        compatibilityType <- storage
          .getSubjectCompatibility(subject)
          .ensure(SubjectDoesNotExist(subject))(_.isDefined)
          .map(_.get)
        isCompatible <- isSchemaCompatible(subject, newSchema, compatibilityType)
      } yield isCompatible
    }
  } yield result

  override def getSubjectSchemas(subject: String): F[List[SchemaMetadata]] = for {
    _ <- Logger[F].info(s"Get last subject schemas: $subject")
    result <- transact(isSubjectExists(subject) *> storage.getSubjectSchemas(subject))
      .ensure(SubjectHasNoRegisteredSchemas(subject))(_.nonEmpty)
  } yield result

  override def registerSchema(schemaText: String, schemaType: SchemaType): F[SchemaId] = for {
    _ <- Logger[F].info(s"Register new schema: $schemaText - ${schemaType.identifier}")
    _ <- validateSchema(schemaText)
    schemaHash <- Utils.toMD5Hex(schemaText).pure[F]
    result <- transact {
      for {
        _ <- storage.schemaByHash(schemaHash).reject { case Some(value) =>
          SchemaIsAlreadyExist(value.getSchemaId, schemaText)
        }
        newSchema <- storage.registerSchema(schemaText, schemaHash, schemaType).map(SchemaId.instance)
      } yield newSchema
    }
  } yield result

  override def registerSchema(
    subject: String,
    schemaText: String,
    compatibilityType: CompatibilityType,
    schemaType: SchemaType
  ): F[SchemaId] = for {
    _ <- Logger[F].info(s"Register schema: $schemaText and add to subject: $subject")
    schema <- validateSchema(schemaText)
    schemaHash <- Utils.toMD5Hex(schemaText).pure[F]
    result <- lock {
      for {
        schemaId <- storage.schemaByHash(schemaHash).flatMap {
          case Some(value) => pure(value.getSchemaId)
          case None        => storage.registerSchema(schemaText, schemaHash, schemaType)
        }
        subjectMeta <- storage.subjectMetadata(subject).flatMap[SubjectMetadata] {
          case Some(meta) if meta.isLocked => raiseErrorF(SubjectIsLocked(subject))
          case Some(meta)                  => pure(meta)
          case None                        => storage.registerSubject(subject, compatibilityType, isLocked = false)
        }
        _ <- isSchemaCompatible(subject, schema, subjectMeta.getCompatibilityType)
          .ensure(SchemaIsNotCompatible(subject, schemaText, subjectMeta.getCompatibilityType))(identity)
        _ <- storage
          .isSubjectConnectedToSchema(subject, schemaId)
          .ensure(SubjectIsAlreadyConnectedToSchema(subject, schemaId))(f => !f)
        nextVersion <- storage.getNextVersionNumber(subject)
        _ <- storage.addSchemaToSubject(subject, schemaId, nextVersion)
      } yield SchemaId.instance(schemaId)
    }
  } yield result

  override def registerSubject(
    subject: String,
    compatibilityType: CompatibilityType,
    isLocked: Boolean
  ): F[SubjectMetadata] = for {
    _ <- Logger[F].info(s"Register new subject: $subject, ${compatibilityType.identifier}")
    result <- transact(storage.registerSubject(subject, compatibilityType, isLocked))
  } yield result

  override def addSchemaToSubject(subject: String, schemaId: Int): F[Int] = for {
    _ <- Logger[F].info(s"Add schema: $schemaId to subject: $subject")
    result <- lock {
      for {
        meta <- storage.subjectMetadata(subject).flatMap[SubjectMetadata] {
          case None                        => raiseErrorF(SubjectDoesNotExist(subject))
          case Some(meta) if meta.isLocked => raiseErrorF(SubjectIsLocked(subject))
          case Some(meta)                  => pure(meta)
        }
        schemaMeta <- storage.schemaById(schemaId).flatMap[SchemaMetadata] {
          case None             => raiseErrorF(SchemaIdDoesNotExist(schemaId))
          case Some(schemaMeta) => pure(schemaMeta)
        }
        _ <- storage
          .isSubjectConnectedToSchema(subject, schemaId)
          .ensure(SubjectIsAlreadyConnectedToSchema(subject, schemaId))(f => !f)
        _ <- isSchemaCompatible(subject, schemaMeta.getSchema, meta.getCompatibilityType)
          .ensure(SchemaIsNotCompatible(subject, schemaMeta.getSchemaText, meta.getCompatibilityType))(identity)
        nextVersion <- storage.getNextVersionNumber(subject)
        _ <- storage.addSchemaToSubject(subject, schemaId, nextVersion)
      } yield nextVersion
    }
  } yield result

  private def validateSchema(schemaText: String): F[Schema] =
    F.catchNonFatal(AvroSchemaUtils.parseSchema(schemaText)).adaptError { case _ =>
      SchemaIsNotValid(schemaText)
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

  private def lock[A](fa: ConnectionIO[A]): F[A] =
    transact(storageLock.lockForUpdate() *> fa <* storageLock.unlock()).onSqlException(transact(storageLock.unlock()))

  private def isSubjectExists(subject: String): ConnectionIO[Boolean] =
    storage.isSubjectExist(subject).ensure(SubjectDoesNotExist(subject))(identity)

  private def getLastSchemaParsed(subject: String): ConnectionIO[Option[Schema]] =
    storage.getLastSubjectSchema(subject).map(_.map(_.getSchema))

  private def getLastSchemasParsed(subject: String): ConnectionIO[List[Schema]] =
    storage.getSubjectSchemas(subject).map(_.map(_.getSchema))

  private def pure[A](a: A): Free[connection.ConnectionOp, A] = Free.pure(a)

  private def raiseErrorF[A](err: Throwable): Free[connection.ConnectionOp, A] = connection.raiseError(err)
}

object DBBackedService {
  def create[F[_]: Sync](
    storage: SchemaStorage[ConnectionIO],
    transact: ConnectionIO ~> F,
    storageLock: StorageLock[ConnectionIO]
  ): DBBackedService[F] =
    new DBBackedService(storage, transact, storageLock)
}

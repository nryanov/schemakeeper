package schemakeeper.server.service

import cats.free.Free
import doobie.ConnectionIO
import doobie.free.connection
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.datasource.migration.FlywayMigrationTool
import schemakeeper.server.util.Utils
import doobie.implicits._
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{AvroSchemaCompatibility, AvroSchemaUtils, CompatibilityType, SchemaType}

import scala.collection.JavaConverters._
import DBBackedService._
import cats.Applicative


class DBBackedService[F[_] : Applicative](config: Configuration) extends Service[F] {
  val datasource = DataSource.build(config)
  val storage = DatabaseStorage(config)

  override def subjects(): F[List[String]] = {
    logger.info("Select subject list")
    transaction(storage.subjects())
  }

  override def subjectVersions(subject: String): F[List[Int]] = {
    logger.info(s"Select subject versions: $subject")
    transaction(storage.subjectVersions(subject))
  }

  override def subjectSchemaByVersion(subject: String, version: Int): F[Option[SchemaMetadata]] = {
    logger.info(s"Select subject schema metadata by version: $subject:$version")
    transaction(storage.subjectSchemaByVersion(subject, version))
  }

  override def subjectOnlySchemaByVersion(subject: String, version: Int): F[Option[String]] = {
    logger.info(s"Select subject schema by version: $subject:$version")
    transaction(storage.subjectOnlySchemaByVersion(subject, version))
  }

  override def schemaById(id: Int): F[Option[String]] = {
    logger.info(s"Select schema by id: $id")
    transaction(storage.schemaById(id))
  }

  override def deleteSubject(subject: String): F[Boolean] = {
    logger.info(s"Delete subject: $subject")
    transaction(storage.deleteSubject(subject))
  }

  override def deleteSubjectVersion(subject: String, version: Int): F[Boolean] = {
    logger.info(s"Delete subject version: $subject:$version")
    transaction(storage.deleteSubjectVersion(subject, version))
  }

  override def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Option[CompatibilityType]] = {
    logger.info(s"Update subject compatibility: $subject - ${compatibilityType.name()}")
    transaction(storage.updateSubjectCompatibility(subject, compatibilityType))
  }

  override def getSubjectCompatibility(subject: String): F[Option[CompatibilityType]] = {
    logger.info(s"Get subject compatibility: $subject")
    transaction(storage.getSubjectCompatibility(subject))
  }

  override def getLastSchema(subject: String): F[Option[String]] = {
    logger.info(s"Get subject last schema: $subject")
    transaction(storage.getLastSchema(subject))
  }

  override def getLastSchemas(subject: String): F[List[String]] = {
    logger.info(s"Get subject last schemas: $subject")
    transaction(storage.getLastSchemas(subject))
  }

  override def checkSubjectSchemaCompatibility(subject: String, schema: String): F[Boolean] = {
    logger.info(s"Check subject schema compatibility: $subject - $schema")

    val query = for {
      compatibility <- storage.getSubjectCompatibility(subject)
      r <- if (compatibility.isDefined) isSchemaCompatible(subject, AvroSchemaUtils.parseSchema(schema), compatibility.get) else Free.pure[connection.ConnectionOp, Boolean](false)
    } yield r

    transaction(query)
  }

  // todo: lock all subject schemas for update
  override def registerNewSubjectSchema(subject: String, schema: String, schemaType: SchemaType): F[Int] = {
    logger.info(s"Register new subject schema: $subject - $schema")

    val query = storage.checkSubjectExistence(subject)
      .flatMap(isExist => {
        if (isExist) {
          Free.pure[connection.ConnectionOp, Int](0)
        } else {
          storage.getGlobalCompatibility().flatMap(globalCompatibilityType =>
            storage.registerNewSubject(subject, schemaType, globalCompatibilityType.getOrElse(CompatibilityType.BACKWARD))
          )
        }
      })
      .flatMap(_ => storage.getSubjectCompatibility(subject))
      .flatMap(compatibilityTypeOption => Free.pure[connection.ConnectionOp, CompatibilityType](compatibilityTypeOption.get))
      .flatMap(compatibilityType => isSchemaCompatible(subject, AvroSchemaUtils.parseSchema(schema), compatibilityType))
      .map(flag => {
        if (!flag) {
          throw new Exception("New schema is not compatible")
        }
      })
      .flatMap(_ => storage.getNextVersionNumber(subject))
      .flatMap(nextVersion => storage.registerNewSubjectSchema(subject, schema, schemaType, nextVersion, Utils.toMD5Hex(schema)))

    transaction(query)
  }

  override def registerNewSubject(subject: String, schema: String, schemaType: SchemaType, compatibilityType: CompatibilityType): F[Option[Int]] = {
    logger.info(s"Register new subject: $subject [${schemaType.identifier}, ${compatibilityType.identifier}]")

    val query = storage.checkSubjectExistence(subject).flatMap(isExist => {
      if (isExist) {
        Free.pure[connection.ConnectionOp, Option[Int]](None)
      } else {
        storage.registerNewSubject(subject, schemaType, compatibilityType)
          .flatMap(_ => storage.registerNewSubjectSchema(subject, schema, schemaType, 1, Utils.toMD5Hex(schema)))
          .map(Option(_))
      }
    })

    transaction(query)
  }

  //todo: refactor
  override def getSubjectMetadata(subject: String): F[Option[SubjectMetadata]] = {
    logger.info(s"Getting subject metadata: $subject")
    val query = storage.getSubject(subject)
      .flatMap(subjectMeta => storage.subjectVersions(subject)
        .map(versions => subjectMeta
          .map(meta => {
            meta.setVersions(versions.toArray)
            meta
          })))

    transaction(query)
  }

  override def getGlobalCompatibility(): F[Option[CompatibilityType]] = {
    logger.info("Get global compatibility type")
    transaction(storage.getGlobalCompatibility())
  }

  override def updateGlobalCompatibility(compatibilityType: CompatibilityType): F[Option[CompatibilityType]] = {
    logger.info(s"Update global compatibility type to: ${compatibilityType.identifier}")
    transaction(storage.updateGlobalCompatibility(compatibilityType))
  }

  private def transaction[A](query: ConnectionIO[A]): F[A] = Applicative[F].pure {
    datasource.use {
      xa => query.transact(xa)
    }.unsafeRunSync()
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

  private def getLastSchemaParsed(subject: String): ConnectionIO[Option[Schema]] = storage.getLastSchema(subject).map(_.map(AvroSchemaUtils.parseSchema))

  private def getLastSchemasParsed(subject: String): ConnectionIO[List[Schema]] = storage.getLastSchemas(subject).map(_.map(AvroSchemaUtils.parseSchema))
}

object DBBackedService {
  private val logger = LoggerFactory.getLogger(DBBackedService.getClass)

  def apply[F[_] : Applicative](config: Configuration): DBBackedService[F] = {
    FlywayMigrationTool.migrate(config)
    new DBBackedService[F](config)
  }
}

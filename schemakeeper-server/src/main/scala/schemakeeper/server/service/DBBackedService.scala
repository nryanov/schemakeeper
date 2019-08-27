package schemakeeper.server.service

import cats.Applicative
import cats.free.Free
import doobie.ConnectionIO
import doobie.free.connection
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSource
import schemakeeper.server.metadata.AvroSchemaMetadata
import schemakeeper.server.storage.DatabaseStorage
import schemakeeper.server.storage.migration.FlywayMigrationTool
import schemakeeper.server.util.Utils
import doobie._
import doobie.implicits._
import org.apache.avro.Schema
import schemakeeper.schema.{AvroSchemaCompatibility, AvroSchemaUtils, CompatibilityType}


class DBBackedService[F[_]: Applicative](config: Configuration) extends Service[F] {
  val datasource = DataSource.build(config)
  val storage = DatabaseStorage()

  override def subjects(): F[List[String]] =
    transaction(storage.subjects())

  override def subjectVersions(subject: String): F[List[Int]] =
    transaction(storage.subjectVersions(subject))

  override def subjectSchemaByVersion(subject: String, version: Int): F[Option[AvroSchemaMetadata]] =
    transaction(storage.subjectSchemaByVersion(subject, version))

  override def subjectOnlySchemaByVersion(subject: String, version: Int): F[Option[String]] =
    transaction(storage.subjectOnlySchemaByVersion(subject, version))

  override def schemaById(id: Int): F[Option[String]] =
    transaction(storage.schemaById(id))

  override def deleteSubject(subject: String): F[Boolean] =
    transaction(storage.deleteSubject(subject))

  override def deleteSubjectVersion(subject: String, version: Int): F[Boolean] =
    transaction(storage.deleteSubjectVersion(subject, version))

  override def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Boolean] =
    transaction(storage.updateSubjectCompatibility(subject, compatibilityType))

  override def getSubjectCompatibility(subject: String): F[Option[CompatibilityType]] =
    transaction(storage.getSubjectCompatibility(subject))

  override def getLastSchema(subject: String): F[Option[String]] =
    transaction(storage.getLastSchema(subject))

  override def getLastSchemas(subject: String): F[List[String]] =
    transaction(storage.getLastSchemas(subject))

  // todo: lock all subject schemas for update
  override def registerNewSubjectSchema(subject: String, schema: String): F[Int] = {
    val query = storage.checkSubjectExistence(subject)
      .flatMap(isExist => {
        if (isExist) {
          Free.pure[connection.ConnectionOp, Int](0)
        } else {
          storage.registerNewSubject(subject)
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
      .flatMap(nextVersion => storage.registerNewSubjectSchema(subject, schema, nextVersion, Utils.toMD5Hex(schema)))

    transaction(query)
  }

  private def transaction[A](query: ConnectionIO[A]): F[A] = Applicative[F].pure {
    datasource.use {
      xa => query.transact(xa)
    }.unsafeRunSync()
  }

  private def isSchemaCompatible(subject: String, newSchema: Schema, compatibilityType: CompatibilityType): ConnectionIO[Boolean] = compatibilityType match {
    case CompatibilityType.None => Free.pure[connection.ConnectionOp, Boolean](true)
    case CompatibilityType.Backward => getLastSchemaParsed(subject).map(_.forall(previousSchema => AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema)))
    case CompatibilityType.Forward => getLastSchemaParsed(subject).map(_.forall(previousSchema => AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(newSchema, previousSchema)))
    case CompatibilityType.Full => getLastSchemaParsed(subject).map(_.forall(previousSchema => AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(newSchema, previousSchema)))
    case CompatibilityType.BackwardTransitive => getLastSchemasParsed(subject).map(previousSchemas => AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas))
    case CompatibilityType.ForwardTransitive => getLastSchemasParsed(subject).map(previousSchemas => AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas))
    case CompatibilityType.FullTransitive => getLastSchemasParsed(subject).map(previousSchemas => AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(newSchema, previousSchemas))
  }

  private def getLastSchemaParsed(subject: String): ConnectionIO[Option[Schema]] = storage.getLastSchema(subject).map(_.map(AvroSchemaUtils.parseSchema))

  private def getLastSchemasParsed(subject: String): ConnectionIO[List[Schema]] = storage.getLastSchemas(subject).map(_.map(AvroSchemaUtils.parseSchema))
}

object DBBackedService {
  def apply[F[_]: Applicative](config: Configuration): DBBackedService[F] = {
    FlywayMigrationTool.migrate(config)
    new DBBackedService[F](config)
  }
}

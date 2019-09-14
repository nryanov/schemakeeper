package schemakeeper.server.storage

import doobie._
import doobie.quill.DoobieContext
import io.getquill.SnakeCase
import schemakeeper.api.SchemaMetadata
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.Configuration
import schemakeeper.server.datasource.DataSourceUtils
import schemakeeper.server.datasource.migration.SupportedDatabaseProvider
import schemakeeper.server.storage.model.{Config, SchemaInfo, Subject}
import schemakeeper.server.storage.model.Converters._

class DatabaseStorage(configuration: Configuration) extends SchemaStorage[ConnectionIO] {
  private implicit val logHandler: LogHandler = LogHandler.jdkLogHandler
  private val dc = DatabaseStorage.context(configuration)

  import dc._

  implicit private val schemaInfoInsertMeta = insertMeta[SchemaInfo](_.id)

  override def schemaById(id: Int): ConnectionIO[Option[String]] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.id == lift(id))
      .map(_.schemaText)
  }).map(_.headOption)

  override def subjects(): ConnectionIO[List[String]] = dc.run(quote {
    query[Subject]
      .map(_.subjectName)
  })

  override def subjectVersions(subject: String): ConnectionIO[List[Int]] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.subjectName == lift(subject))
      .sortBy(_.version)(Ord.asc)
      .map(_.version)
  })

  override def subjectSchemaByVersion(subject: String, version: Int): ConnectionIO[Option[SchemaMetadata]] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.subjectName == lift(subject))
      .filter(_.version == lift(version))
  }).map(_.headOption.map(schemaInfoToSchemaMetadata))

  override def subjectOnlySchemaByVersion(subject: String, version: Int): ConnectionIO[Option[String]] =
    subjectSchemaByVersion(subject, version)
      .map(_.map(_.getSchemaText))

  override def deleteSubject(subject: String): ConnectionIO[Boolean] = dc.run(quote {
    query[Subject]
      .filter(_.subjectName == lift(subject))
      .delete
  }).map(_ > 0)

  override def deleteSubjectVersion(subject: String, version: Int): ConnectionIO[Boolean] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.subjectName == lift(subject))
      .filter(_.version == lift(version))
      .delete
  }).map(_ > 0)

  override def registerNewSubjectSchema(subject: String, schema: String, schemaType: SchemaType, version: Int, schemaHash: String): ConnectionIO[Int] = dc.run(quote {
    query[SchemaInfo]
      .insert(lift(SchemaInfo(0, version, schemaType.identifier, subject, schema, schemaHash)))
      .returning(_.id)
  })

  override def checkSubjectExistence(subject: String): ConnectionIO[Boolean] = dc.run(quote {
    query[Subject]
      .filter(_.subjectName == lift(subject))
      .nonEmpty
  })

  override def registerNewSubject(subject: String, schemaType: SchemaType, compatibilityType: CompatibilityType): ConnectionIO[Int] = dc.run(quote {
    query[Subject]
      .insert(lift(Subject(subject, schemaType.identifier, compatibilityType.identifier)))
  }).map(_.toInt)

  override def getNextVersionNumber(subject: String): ConnectionIO[Int] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.subjectName == lift(subject))
      .sortBy(_.version)(Ord.desc)
      .map(_.version)
  }).map(_.headOption).map(_.map(_ + 1).getOrElse(1))

  override def getLastSchema(subject: String): ConnectionIO[Option[String]] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.subjectName == lift(subject))
      .sortBy(_.version)(Ord.desc)
      .map(_.schemaText)
  }).map(_.headOption)

  override def getLastSchemas(subject: String): ConnectionIO[List[String]] = dc.run(quote {
    query[SchemaInfo]
      .filter(_.subjectName == lift(subject))
      .sortBy(_.version)(Ord.desc)
      .map(_.schemaText)
  })

  override def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): ConnectionIO[Option[CompatibilityType]] = dc.run(quote {
    query[Subject]
      .filter(_.subjectName == lift(subject))
      .update(_.compatibilityTypeName -> lift(compatibilityType.identifier))
  }).map(_ > 0)
    .map(f => if (f) Some(compatibilityType) else None)

  override def getSubjectCompatibility(subject: String): ConnectionIO[Option[CompatibilityType]] = dc.run(quote {
    query[Subject]
      .filter(_.subjectName == lift(subject))
      .map(_.compatibilityTypeName)
  }).map(_.headOption)
    .map(_.map(CompatibilityType.findByName))

  override def getGlobalCompatibility(): ConnectionIO[Option[CompatibilityType]] = dc.run(quote {
    query[Config]
      .filter(_.configName == "default.compatibility")
      .map(_.configName)
  }).map(_.headOption)
    .map(_.map(CompatibilityType.findByName))

  override def updateGlobalCompatibility(compatibilityType: CompatibilityType): ConnectionIO[Option[CompatibilityType]] = dc.run(quote {
    query[Config]
      .filter(_.configName == "default.compatibility")
      .update(_.configValue -> lift(compatibilityType.identifier))
  }).map(_ > 0)
    .map(f => if (f) Some(compatibilityType) else None)
}

object DatabaseStorage {
  def apply(configuration: Configuration): DatabaseStorage = new DatabaseStorage(configuration)

  def context(configuration: Configuration) = DataSourceUtils.detectDatabaseProvider(configuration.databaseConnectionString) match {
    case SupportedDatabaseProvider.PostgreSQL => new DoobieContext.Postgres(SnakeCase)
    case SupportedDatabaseProvider.MySQL => new DoobieContext.MySQL(SnakeCase)
    case SupportedDatabaseProvider.H2 => new DoobieContext.H2(SnakeCase)
  }
}
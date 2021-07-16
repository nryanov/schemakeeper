package schemakeeper.server.storage

import doobie._
import doobie.free.connection
import doobie.implicits._
import doobie.quill.DoobieContextBase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill._
import schemakeeper.api.{SchemaMetadata, SubjectMetadata, SubjectSchemaMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.SchemaKeeperError._
import schemakeeper.server.storage.exception.StorageExceptionHandler
import schemakeeper.server.storage.model.{SchemaInfo, Subject, SubjectSchema}
import schemakeeper.server.storage.model.Converters._

class DatabaseStorage(
  dc: DoobieContextBase[_ <: SqlIdiom, _ <: NamingStrategy],
  storageExceptionHandler: StorageExceptionHandler
) extends SchemaStorage[ConnectionIO] {
  private implicit val logHandler: LogHandler = LogHandler.jdkLogHandler

  import dc._

  implicit private val schemaInfoInsertMeta = insertMeta[SchemaInfo](_.schemaId)

  override def subjects(): doobie.ConnectionIO[List[String]] = dc.run(quote {
    query[Subject].map(_.subjectName)
  })

  override def subjectMetadata(subject: String): doobie.ConnectionIO[Option[SubjectMetadata]] = dc
    .run(quote {
      query[Subject].filter(_.subjectName == lift(subject))
    })
    .map(_.headOption)
    .map(_.map(subjectInfoToSubjectMetadata))

  override def subjectVersions(subject: String): doobie.ConnectionIO[List[Int]] = dc.run(quote {
    query[SubjectSchema].filter(_.subjectName == lift(subject)).map(_.version)
  })

  override def updateSubjectSettings(
    subject: String,
    compatibilityType: CompatibilityType,
    isLocked: Boolean
  ): doobie.ConnectionIO[SubjectMetadata] = dc
    .run(quote {
      query[Subject]
        .filter(_.subjectName == lift(subject))
        .update(
          _.isLocked -> lift(isLocked),
          _.compatibilityTypeName -> lift(compatibilityType.identifier)
        )
    })
    .map(_ => SubjectMetadata.instance(subject, compatibilityType, isLocked))

  override def subjectSchemasMetadata(subject: String): doobie.ConnectionIO[List[SubjectSchemaMetadata]] = dc
    .run(quote {
      query[SubjectSchema]
        .join(query[SchemaInfo])
        .on(_.schemaId == _.schemaId)
        .filter(_._1.subjectName == lift(subject))
    })
    .map(
      _.map(meta =>
        SubjectSchemaMetadata.instance(
          meta._1.schemaId,
          meta._1.version,
          meta._2.schemaText,
          meta._2.schemaHash,
          SchemaType.findByName(meta._2.schemaTypeName)
        )
      )
    )

  override def subjectSchemaByVersion(
    subject: String,
    version: Index
  ): doobie.ConnectionIO[Option[SubjectSchemaMetadata]] = dc
    .run(quote {
      query[SubjectSchema]
        .join(query[SchemaInfo])
        .on(_.schemaId == _.schemaId)
        .filter(_._1.subjectName == lift(subject))
        .filter(_._1.version == lift(version))
    })
    .map(_.headOption)
    .map(
      _.map(meta =>
        SubjectSchemaMetadata.instance(
          meta._1.schemaId,
          meta._1.version,
          meta._2.schemaText,
          meta._2.schemaHash,
          SchemaType.findByName(meta._2.schemaTypeName)
        )
      )
    )

  override def schemaById(id: Int): doobie.ConnectionIO[Option[SchemaMetadata]] = dc
    .run(quote {
      query[SchemaInfo].filter(_.schemaId == lift(id))
    })
    .map(_.headOption)
    .map(
      _.map(meta =>
        SchemaMetadata
          .instance(meta.schemaId, meta.schemaText, meta.schemaHash, SchemaType.findByName(meta.schemaTypeName))
      )
    )

  override def schemaByHash(schemaHash: String): doobie.ConnectionIO[Option[SchemaMetadata]] = dc
    .run(quote {
      query[SchemaInfo].filter(_.schemaHash == lift(schemaHash))
    })
    .map(_.headOption)
    .map(
      _.map(meta =>
        SchemaMetadata
          .instance(meta.schemaId, meta.schemaText, meta.schemaHash, SchemaType.findByName(meta.schemaTypeName))
      )
    )

  override def deleteSubject(subject: String): doobie.ConnectionIO[Boolean] = dc
    .run(quote {
      query[Subject].filter(_.subjectName == lift(subject)).delete
    })
    .map(_ > 0)

  override def deleteSubjectSchemaByVersion(subject: String, version: Index): doobie.ConnectionIO[Boolean] = dc
    .run(quote {
      query[SubjectSchema].filter(_.subjectName == lift(subject)).filter(_.version == lift(version)).delete
    })
    .map(_ > 0)

  override def getSubjectCompatibility(subject: String): doobie.ConnectionIO[Option[CompatibilityType]] = dc
    .run(quote {
      query[Subject].filter(_.subjectName == lift(subject)).map(_.compatibilityTypeName)
    })
    .map(_.headOption)
    .map(_.map(CompatibilityType.findByName))

  override def getLastSubjectSchema(subject: String): doobie.ConnectionIO[Option[SchemaMetadata]] = dc
    .run(quote {
      query[SchemaInfo].filter(schemaInfo =>
        query[SubjectSchema]
          .filter(_.subjectName == lift(subject))
          .filter(subjectSchema =>
            query[SubjectSchema]
              .filter(_.subjectName == lift(subject))
              .map(_.version)
              .max
              .contains(subjectSchema.version)
          )
          .map(_.schemaId)
          .contains(schemaInfo.schemaId)
      )
    })
    .map(_.headOption)
    .map(
      _.map(meta =>
        SchemaMetadata
          .instance(meta.schemaId, meta.schemaText, meta.schemaHash, SchemaType.findByName(meta.schemaTypeName))
      )
    )

  override def getSubjectSchemas(subject: String): doobie.ConnectionIO[List[SchemaMetadata]] = dc
    .run(quote {
      query[SubjectSchema]
        .join(query[SchemaInfo])
        .on(_.schemaId == _.schemaId)
        .filter(_._1.subjectName == lift(subject))
        .map(_._2)
    })
    .map(
      _.map(meta =>
        SchemaMetadata
          .instance(meta.schemaId, meta.schemaText, meta.schemaHash, SchemaType.findByName(meta.schemaTypeName))
      )
    )

  override def registerSchema(schema: String, schemaHash: String, schemaType: SchemaType): doobie.ConnectionIO[Int] =
    dc.run(quote {
        query[SchemaInfo]
          .insert(lift(SchemaInfo(0, schemaType.identifier, schema, schemaHash)))
          .returningGenerated(_.schemaId)
      })
      .exceptSql {
        case err if storageExceptionHandler.isUniqueViolation(err) =>
          connection.raiseError(SchemaIsAlreadyExist(-1, schema))
        case err => connection.raiseError(BackendError(err))
      }

  override def registerSubject(
    subject: String,
    compatibilityType: CompatibilityType,
    isLocked: Boolean
  ): doobie.ConnectionIO[SubjectMetadata] = dc
    .run(quote {
      query[Subject].insert(lift(Subject(subject, compatibilityType.identifier, isLocked)))
    })
    .exceptSql {
      case err if storageExceptionHandler.isUniqueViolation(err) =>
        connection.raiseError(SubjectIsAlreadyExists(subject))
      case err => connection.raiseError(BackendError(err))
    }
    .map(_ => SubjectMetadata.instance(subject, compatibilityType, isLocked))

  override def addSchemaToSubject(subject: String, schemaId: Index, version: Index): doobie.ConnectionIO[Unit] = dc
    .run(quote {
      query[SubjectSchema].insert(lift(SubjectSchema(subject, schemaId, version)))
    })
    .map(_ => Unit)

  override def isSubjectExist(subject: String): doobie.ConnectionIO[Boolean] = dc.run(quote {
    query[Subject].filter(_.subjectName == lift(subject)).nonEmpty
  })

  override def isSubjectConnectedToSchema(subject: String, schemaId: Index): doobie.ConnectionIO[Boolean] =
    dc.run(quote {
      query[SubjectSchema].filter(_.subjectName == lift(subject)).filter(_.schemaId == lift(schemaId)).nonEmpty
    })

  override def getNextVersionNumber(subject: String): doobie.ConnectionIO[Int] = dc
    .run(quote {
      query[SubjectSchema].filter(_.subjectName == lift(subject)).sortBy(_.version)(Ord.desc).map(_.version)
    })
    .map(_.headOption)
    .map(_.map(_ + 1).getOrElse(1))
}

object DatabaseStorage {
  def create(
    dc: DoobieContextBase[_ <: SqlIdiom, _ <: NamingStrategy],
    storageExceptionHandler: StorageExceptionHandler
  ): DatabaseStorage = new DatabaseStorage(dc, storageExceptionHandler)
}

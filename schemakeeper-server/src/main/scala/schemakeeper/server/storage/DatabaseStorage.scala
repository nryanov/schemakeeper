package schemakeeper.server.storage

import schemakeeper.avro.compatibility.CompatibilityType
import schemakeeper.server.metadata.AvroSchemaMetadata
import doobie._
import doobie.implicits._

class DatabaseStorage() extends SchemaStorage[ConnectionIO] {
  override def schemaById(id: Int): ConnectionIO[Option[String]] =
    sql"select schema_text from schemakeeper.schema_info where id = $id"
      .query[String]
      .option

  override def subjects(): ConnectionIO[List[String]] =
    sql"select subject_name from schemakeeper.subject"
      .query[String]
      .to[List]

  override def subjectVersions(subject: String): ConnectionIO[List[Int]] =
    sql"select version from schemakeeper.schema_info where subject_name = $subject"
      .query[Int]
      .to[List]

  override def subjectSchemaByVersion(subject: String, version: Int): ConnectionIO[Option[AvroSchemaMetadata]] =
    sql"select subject_name, id, version, schema_text from schemakeeper.schema_info where subject_name = $subject and version = $version"
      .query[AvroSchemaMetadata]
      .option

  override def subjectOnlySchemaByVersion(subject: String, version: Int): ConnectionIO[Option[String]] =
    subjectSchemaByVersion(subject, version).map(_.map(_.schema))

  override def deleteSubject(subject: String): ConnectionIO[Boolean] =
    sql"delete from schemakeeper.subject where subject_name = $subject"
      .update
      .run
      .map(_ > 0)

  override def deleteSubjectVersion(subject: String, version: Int): ConnectionIO[Boolean] =
    sql"delete from schemakeeper.schema_info where subject_name = $subject and version = $version"
      .update
      .run
      .map(_ > 0)

  override def registerNewSubjectSchema(subject: String, schema: String, version: Int, schemaHash: String): ConnectionIO[Int] =
    sql"""insert into schemakeeper.schema_info(version, schema_type_name, subject_name, schema_text, schema_hash)
         |values ($version, 'avro', $subject, $schema, $schemaHash)"""
      .stripMargin
      .update
      .withUniqueGeneratedKeys[Int]("id")

  override def checkSubjectExistence(subject: String): ConnectionIO[Boolean] =
    sql"select exists (select 1 from schemakeeper.subject where subject_name = $subject)"
      .query[Boolean]
      .unique

  //todo: get compatibility type from global config
  override def registerNewSubject(subject: String): ConnectionIO[Int] =
    sql"insert into schemakeeper.subject(subject_name, schema_type_name, compatibility_type_name) values ($subject, 'avro', 'backward')"
      .update
      .run

  override def getNextVersionNumber(subject: String): ConnectionIO[Int] =
    sql"select max(version) from schemakeeper.schema_info where subject_name = $subject"
      .query[Int]
      .option
      .map(_.map(_ + 1).getOrElse(1))

  override def getLastSchema(subject: String): ConnectionIO[Option[String]] =
    sql"select schema_text from schemakeeper.schema_info where subject_name = $subject limit 1"
      .query[String]
      .option

  override def getLastSchemas(subject: String): ConnectionIO[List[String]] =
    sql"select schema_text from schemakeeper.schema_info where subject_name = $subject"
      .query[String]
      .to[List]

  override def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): ConnectionIO[Boolean] =
    sql"update schemakeeper.subject set compatibility_type_name = ${compatibilityType.entryName.toLowerCase} where subject_name = $subject"
      .update
      .run
      .map(_ > 0)

  override def getSubjectCompatibility(subject: String): ConnectionIO[Option[CompatibilityType]] =
    sql"select compatibility_type_name from schemakeeper.subject where subject_name = $subject"
      .query[String]
      .option
      .map(_.flatMap(CompatibilityType.withNameInsensitiveOption))
}

object DatabaseStorage {
  def apply(): DatabaseStorage = new DatabaseStorage()
}
package schemakeeper.server.service

import schemakeeper.schema.CompatibilityType
import schemakeeper.server.metadata.AvroSchemaMetadata

trait Service[F[_]] {
  def subjects(): F[List[String]]

  def subjectVersions(subject: String): F[List[Int]]

  def subjectSchemaByVersion(subject: String, version: Int): F[Option[AvroSchemaMetadata]]

  def subjectOnlySchemaByVersion(subject: String, version: Int): F[Option[String]]

  def schemaById(id: Int): F[Option[String]]

  def deleteSubject(subject: String): F[Boolean]

  def deleteSubjectVersion(subject: String, version: Int): F[Boolean]

  def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Boolean]

  def getSubjectCompatibility(subject: String): F[Option[CompatibilityType]]

  def getLastSchema(subject: String): F[Option[String]]

  def getLastSchemas(subject: String): F[List[String]]

  def registerNewSubjectSchema(subject: String, schema: String): F[Int]
}

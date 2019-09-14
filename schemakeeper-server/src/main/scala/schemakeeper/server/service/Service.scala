package schemakeeper.server.service

import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}

trait Service[F[_]] {
  /**
    * @return - registered subjects name list
    */
  def subjects(): F[List[String]]

  /**
    * @param subject - subject name
    * @return - registered schema versions
    */
  def subjectVersions(subject: String): F[List[Int]]

  /**
    * @param subject - subject name
    * @param version - schema version
    * @return - schema metadata for specified subject and version or none if subject or version do not exist
    */
  def subjectSchemaByVersion(subject: String, version: Int): F[Option[SchemaMetadata]]

  /**
    * @param subject - subject name
    * @param version - schema version
    * @return - schema string for specified subject and version or none if subject or version do not exist
    */
  def subjectOnlySchemaByVersion(subject: String, version: Int): F[Option[String]]

  /**
    * @param id - schema id
    * @return - schema string or none
    */
  def schemaById(id: Int): F[Option[String]]

  /**
    * @param subject - subject name
    * @return - true if subject was deleted, otherwise false
    */
  def deleteSubject(subject: String): F[Boolean]

  /**
    * Check if passed schema is compatible with other schemas depending on subject's compatibility type
    * @param subject - subject name
    * @param schema - schema string
    * @return - true if schema is compatible, otherwise false
    */
  def checkSubjectSchemaCompatibility(subject: String, schema: String): F[Boolean]

  /**
    * @param subject - subject name
    * @param version - schema version
    * @return - true if schema version was deleted otherwise false
    */
  def deleteSubjectVersion(subject: String, version: Int): F[Boolean]

  /**
    * @param subject - subject name
    * @param compatibilityType - new compatibility type
    * @return - compatiiblity type if type was updated successfully otherwise none
    */
  def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Option[CompatibilityType]]

  /**
    * @param subject - subject name
    * @return - compatibility type or none if subject with specified name does not exist
    */
  def getSubjectCompatibility(subject: String): F[Option[CompatibilityType]]

  def getSubjectMetadata(subject: String): F[Option[SubjectMetadata]]

  /**
    * @param subject - subject name
    * @return - last registered schema of specified subject or none
    */
  def getLastSchema(subject: String): F[Option[String]]

  /**
    * @param subject - subject name
    * @return - last registered schemas of specified subject or none
    */
  def getLastSchemas(subject: String): F[List[String]]

  /**
    * Register new schema for already existing subject
    * @param subject - subject name
    * @param schema - new schema
    * @param schemaType - type of new schema
    * @return - id of new schema
    */
  def registerNewSubjectSchema(subject: String, schema: String, schemaType: SchemaType): F[Int]

  /**
    * Create new subject and register first schema for it
    * @param subject - subject name
    * @param schema - schema
    * @param schemaType - type of schema
    * @param compatibilityType - compatibility type of subject
    * @return - id of registered schema or none if subject is already exist
    */
  def registerNewSubject(subject: String, schema: String, schemaType: SchemaType, compatibilityType: CompatibilityType): F[Option[Int]]

  /**
    * @return - global compatibility type used by default for new subjects.
    * May be none if someone deleted record from storage. In this case BACKWARD is used.
    */
  def getGlobalCompatibility(): F[Option[CompatibilityType]]

  /**
    * @param compatibilityType - new compatibility type
    * @return - new compatibility type if value was updated successfully
    */
  def updateGlobalCompatibility(compatibilityType: CompatibilityType): F[Option[CompatibilityType]]
}

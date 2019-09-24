package schemakeeper.server.storage

import schemakeeper.api.{SchemaMetadata, SubjectMetadata, SubjectSchemaMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}


trait SchemaStorage[F[_]] {
  /**
    * @return - all registered subjects name
    */
  def subjects(): F[List[String]]

  /**
    * @param subject - subject name
    * @return - subject metadata or none
    */
  def subjectMetadata(subject: String): F[Option[SubjectMetadata]]

  /**
    * @param subject - subject name
    * @return - list of subject versions or empty list
    */
  def subjectVersions(subject: String): F[List[Int]]

  /**
    * @param subject - subject name
    * @return - list of subject schemas with metadata or empty list
    */
  def subjectSchemasMetadata(subject: String): F[List[SubjectSchemaMetadata]]

  /**
    * @param subject - subject name
    * @param version - schema version
    * @return - subject schema metadata or none
    */
  def subjectSchemaByVersion(subject: String, version: Int): F[Option[SubjectSchemaMetadata]]

  /**
    * @param id - schema id
    * @return - schema or none
    */
  def schemaById(id: Int): F[Option[SchemaMetadata]]

  /**
    * @param schemaHash - schema hash
    * @return - schema or none
    */
  def schemaByHash(schemaHash: String): F[Option[SchemaMetadata]]

  /**
    * @param subject - subject name
    * @return - true if subject was deleted otherwise false
    */
  def deleteSubject(subject: String): F[Boolean]

  /**
    * @param subject - subject name
    * @param version - schema versions
    * @return - true if subject schema was deleted otherwise false
    */
  def deleteSubjectSchemaByVersion(subject: String, version: Int): F[Boolean]

  /**
    * @param subject - subject name
    * @param compatibilityType - new compatibility type
    * @return - true if compatibility type was updated otherwise false
    */
  def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Boolean]

  /**
    * @param subject - subject name
    * @return - compatibility type of subject or none
    */
  def getSubjectCompatibility(subject: String): F[Option[CompatibilityType]]

  /**
    * @param subject - subject name
    * @return - last subject schema or none
    */
  def getLastSubjectSchema(subject: String): F[Option[SchemaMetadata]]

  /**
    * @param subject - subject name
    * @return - list of subject schemas or empty list
    */
  def getSubjectSchemas(subject: String): F[List[SchemaMetadata]]

  /**
    * @param schema - schema text
    * @param schemaHash - schema hash
    * @param schemaType - schema type
    * @return - schema id
    */
  def registerSchema(schema: String, schemaHash: String, schemaType: SchemaType): F[Int]

  /**
    * @param subject - subject name
    * @param compatibilityType - compatibility type
    */
  def registerSubject(subject: String, compatibilityType: CompatibilityType): F[Unit]

  /**
    * Add already registered schema to subject
    * @param subject - subject name
    * @param schemaId - schema id
    * @param version - next schema version for current subject
    */
  def addSchemaToSubject(subject: String, schemaId: Int, version: Int): F[Unit]

  /**
    * @param subject - subject name
    * @return - true if subject exists otherwise false
    */
  def isSubjectExist(subject: String): F[Boolean]

  /**
    * @param subject - subject name
    * @return - next version number
    */
  def getNextVersionNumber(subject: String): F[Int]

  /**
    * Check if subject already connected with schema with specified id
    * @param subject - subject name
    * @param schemaId - schema id
    * @return - true or false
    */
  def isSubjectConnectedToSchema(subject: String, schemaId: Int): F[Boolean]

  /**
    * Used as default compatibility type for new subjects
    * @return - compatibility type. May return none if option was deleted from table.
    *         In this case the hard-coded default value will be used - BACKWARD
    */
  def getGlobalCompatibility(): F[Option[CompatibilityType]]

  /**
    * @param compatibilityType - new compatibility type
    */
  def updateGlobalCompatibility(compatibilityType: CompatibilityType): F[Boolean]
}
package schemakeeper.server.service

import schemakeeper.api.{SchemaId, SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}

trait Service[F[_]] {
  /**
    * @return - registered subjects name list
    */
  def subjects(): F[Either[SchemaKeeperError, List[String]]]

  /**
    * @param subject - subject name
    * @return - subject metadata or none
    */
  def subjectMetadata(subject: String): F[Either[SchemaKeeperError, SubjectMetadata]]

  /**
    * @param subject - subject name
    * @return - registered schema versions
    */
  def subjectVersions(subject: String): F[Either[SchemaKeeperError, List[Int]]]

  /**
    * @param subject - subject name
    * @return - list of subject schemas with metadata or empty list
    */
  def subjectSchemasMetadata(subject: String): F[Either[SchemaKeeperError, List[SchemaMetadata]]]

  /**
    * @param subject - subject name
    * @param version - schema version
    * @return - schema metadata for specified subject and version or none if subject or version do not exist
    */
  def subjectSchemaByVersion(subject: String, version: Int): F[Either[SchemaKeeperError, SchemaMetadata]]

  /**
    * @param id - schema id
    * @return - schema string or none
    */
  def schemaById(id: Int): F[Either[SchemaKeeperError, SchemaMetadata]]

  /**
    * @param subject - subject name
    * @return - true if subject was deleted, otherwise false
    */
  def deleteSubject(subject: String): F[Either[SchemaKeeperError, Boolean]]

  /**
    * @param subject - subject name
    * @param version - schema versions
    * @return - true if subject schema was deleted otherwise false
    */
  def deleteSubjectSchemaByVersion(subject: String, version: Int): F[Either[SchemaKeeperError, Boolean]]

  /**
    * Check if passed schema is compatible with other schemas depending on subject's compatibility type
    * @param subject - subject name
    * @param schema - schema string
    * @return - true if schema is compatible, otherwise false
    */
  def checkSubjectSchemaCompatibility(subject: String, schema: String): F[Either[SchemaKeeperError, Boolean]]

  /**
    * @param subject - subject name
    * @param compatibilityType - new compatibility type
    * @return - compatiiblity type if type was updated successfully otherwise none
    */
  def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): F[Either[SchemaKeeperError, Boolean]]

  /**
    * @param subject - subject name
    * @return - compatibility type or none if subject with specified name does not exist
    */
  def getSubjectCompatibility(subject: String): F[Either[SchemaKeeperError, CompatibilityType]]

  /**
    * @param subject - subject name
    * @return - last subject schema or none
    */
  def getLastSubjectSchema(subject: String): F[Either[SchemaKeeperError, SchemaMetadata]]

  /**
    * @param subject - subject name
    * @return - list of subject schemas or empty list
    */
  def getSubjectSchemas(subject: String): F[Either[SchemaKeeperError, List[SchemaMetadata]]]

  /**
    * @param schema - schema text
    * @param schemaType - schema type
    * @return - schema id
    */
  def registerSchema(schema: String, schemaType: SchemaType): F[Either[SchemaKeeperError, SchemaId]]

  /**
    * Try to register new subject if not exists, then try to register new schema and then add this schema to subject as new version
    * after compatibility check.
    * This is the default method called by client side for schema registration.
    * This method uses optimistic locking. If schema with new version was registered while this method proceeds, then process will be restarted
    * compatibilityType and schemaType may be ignored if subject and/or schema are already exist in db.
    * @param subject - subject name
    * @param schema - schema text
    * @param compatibilityType - subject compatibility type
    * @param schemaType - schema type
    * @return - schema id
    */
  def registerSchema(subject: String, schema: String, compatibilityType: CompatibilityType, schemaType: SchemaType): F[Either[SchemaKeeperError, SchemaId]]

  /**
    * @param subject - subject name
    * @param compatibilityType - compatibility type
    * @param schemaType - schema type
    */
  def registerSubject(subject: String, compatibilityType: CompatibilityType, schemaType: SchemaType): F[Either[SchemaKeeperError, Unit]]

  /**
    * Add already registered schema to subject if schema is compatible.
    * @param subject - subject name
    * @param schemaId - schema id
    * @return - next version number
    */
  def addSchemaToSubject(subject: String, schemaId: Int): F[Either[SchemaKeeperError, Int]]

  /**
    * @param subject - subject name
    * @return - true if subject exists otherwise false
    */
  def isSubjectExist(subject: String): F[Either[SchemaKeeperError, Boolean]]

  /**
    * @return - global compatibility type used by default for new subjects.
    * May be none if someone deleted record from storage. In this case BACKWARD is used.
    */
  def getGlobalCompatibility(): F[Either[SchemaKeeperError, CompatibilityType]]

  /**
    * @param compatibilityType - new compatibility type
    * @return - new compatibility type if value was updated successfully
    */
  def updateGlobalCompatibility(compatibilityType: CompatibilityType): F[Either[SchemaKeeperError, Boolean]]
}

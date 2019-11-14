package schemakeeper.server.service

import schemakeeper.api.{SchemaId, SchemaMetadata, SubjectMetadata, SubjectSchemaMetadata}
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
    * Update settings for subject
    * @param subject - subject name
    * @param compatibilityType - new compatibilityType
    * @param isLocked - lock/unlock subject
    * @return - updated subject settings
    */
  def updateSubjectSettings(subject: String, compatibilityType: CompatibilityType, isLocked: Boolean): F[Either[SchemaKeeperError, SubjectMetadata]]

  /**
    * @param subject - subject name
    * @return - registered schema versions
    */
  def subjectVersions(subject: String): F[Either[SchemaKeeperError, List[Int]]]

  /**
    * @param subject - subject name
    * @return - list of subject schemas with metadata or empty list
    */
  def subjectSchemasMetadata(subject: String): F[Either[SchemaKeeperError, List[SubjectSchemaMetadata]]]

  /**
    * @param subject - subject name
    * @param version - schema version
    * @return - schema metadata for specified subject and version or none if subject or version do not exist
    */
  def subjectSchemaByVersion(subject: String, version: Int): F[Either[SchemaKeeperError, SubjectSchemaMetadata]]

  /**
    * @param id - schema id
    * @return - schema string or none
    */
  def schemaById(id: Int): F[Either[SchemaKeeperError, SchemaMetadata]]

  /**
    * Get schema id
    * @param subject - subject name
    * @param schema - schema text
    * @return - schema id or SubjectIsNotConnectedToSchema
    */
  def schemaIdBySubjectAndSchema(subject: String, schema: String): F[Either[SchemaKeeperError, SchemaId]]

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
    * @param isLocked - subject lock status
    * @return - subject metadata
    */
  def registerSubject(subject: String, compatibilityType: CompatibilityType, isLocked: Boolean): F[Either[SchemaKeeperError, SubjectMetadata]]

  /**
    * Add already registered schema to subject if schema is compatible.
    * @param subject - subject name
    * @param schemaId - schema id
    * @return - next version number
    */
  def addSchemaToSubject(subject: String, schemaId: Int): F[Either[SchemaKeeperError, Int]]
}

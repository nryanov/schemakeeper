package schemakeeper.server.service

import schemakeeper.schema.CompatibilityType

sealed abstract class SchemaKeeperError(msg: String) {
  def this(e: Throwable) = this(e.getLocalizedMessage)
}

final case class BackendError(e: Throwable) extends SchemaKeeperError(e)

final case class SubjectDoesNotExist(subject: String) extends SchemaKeeperError(s"Subject $subject does not exist")

final case class SubjectIsAlreadyExists(subject: String) extends SchemaKeeperError(s"Subject $subject already exists")

final case class SubjectHasNoRegisteredSchemas(subject: String) extends SchemaKeeperError(s"Subject: $subject has no registered schemas")

final case class SubjectSchemaVersionDoesNotExist(subject: String, version: Int) extends SchemaKeeperError(s"Subject: $subject does not have schema with version: $version")

final case class SchemaDoesNotExist(schemaId: Int) extends SchemaKeeperError(s"Schema with id: $schemaId does not exist")

final case class ConfigIsNotDefined(configName: String) extends SchemaKeeperError(s"Config value for $configName is not defined")

final case class SchemaIsNotValid(schema: String) extends SchemaKeeperError(s"Schema: $schema is not a valid Avro schema")

final case class SchemaIsAlreadyExist(schemaId: Int, schema: String) extends SchemaKeeperError(s"Schema: $schema is already exist")

final case class SubjectIsAlreadyConnectedToSchema(subject: String, schemaId: Int) extends SchemaKeeperError(s"Subject: $subject is already connected to schema: $schemaId")

final case class SchemaIsNotCompatible(
                                        subject: String,
                                        schemaText: String,
                                        compatibilityType: CompatibilityType) extends
  SchemaKeeperError(s"New schema: $schemaText is not compatible with previous for subject: $subject with compatibility type: ${compatibilityType.identifier}")
package schemakeeper.server.storage.model

import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}

object Converters {
  def schemaInfoToSchemaMetadata(schemaInfo: SchemaInfo): SchemaMetadata =
    SchemaMetadata.instance(schemaInfo.schemaId, schemaInfo.schemaText, schemaInfo.schemaHash, SchemaType.findByName(schemaInfo.schemaTypeName))

  def subjectInfoToSubjectMetadata(subject: Subject): SubjectMetadata =
    SubjectMetadata.instance(subject.subjectName, CompatibilityType.findByName(subject.compatibilityTypeName), subject.isLocked)
}

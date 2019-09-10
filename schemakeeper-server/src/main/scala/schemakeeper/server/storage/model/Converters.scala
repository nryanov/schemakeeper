package schemakeeper.server.storage.model

import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.server.util.Utils

object Converters {
  def schemaInfoToSchemaMetadata(schemaInfo: SchemaInfo): SchemaMetadata =
    SchemaMetadata.instance(schemaInfo.subjectName, schemaInfo.id, schemaInfo.version, schemaInfo.schemaText)

  def subjectInfoToSubjectMetadata(subject: Subject): SubjectMetadata =
    SubjectMetadata.instance(subject.subjectName, Utils.compatibilityTypeFromStringUnsafe(subject.compatibilityTypeName), subject.schemaTypeName)
}

package schemakeeper.server.storage.model

import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{CompatibilityType, SchemaType}

object Converters {
  def schemaInfoToSchemaMetadata(schemaInfo: SchemaInfo): SchemaMetadata =
    SchemaMetadata.instance(schemaInfo.subjectName, schemaInfo.id, schemaInfo.version, schemaInfo.schemaText)

  def subjectInfoToSubjectMetadata(subject: Subject): SubjectMetadata =
    SubjectMetadata.instance(subject.subjectName, CompatibilityType.findByName(subject.compatibilityTypeName), SchemaType.findByName(subject.schemaTypeName))

  def subjectInfoToSubjectMetadata(subject: Subject, versions: Array[Int]): SubjectMetadata =
    SubjectMetadata.instance(subject.subjectName, CompatibilityType.findByName(subject.compatibilityTypeName), SchemaType.findByName(subject.schemaTypeName), versions)
}

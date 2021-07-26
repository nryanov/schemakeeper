package schemakeeper.server.storage.model

private[storage] case class Subject(subjectName: String, compatibilityTypeName: String, isLocked: Boolean = false)

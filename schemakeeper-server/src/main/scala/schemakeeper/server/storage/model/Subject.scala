package schemakeeper.server.storage.model

private[storage] case class Subject(
                                     subjectName: String,
                                     schemaTypeName: String,
                                     compatibilityTypeName: String
                                   )
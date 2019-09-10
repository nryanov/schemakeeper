package schemakeeper.server.storage.model

private[storage] case class SchemaInfo(
                       id: Int,
                       version: Int,
                       schemaTypeName: String,
                       subjectName: String,
                       schemaText: String,
                       schemaHash: String
                     )

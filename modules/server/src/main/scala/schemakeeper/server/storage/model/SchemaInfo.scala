package schemakeeper.server.storage.model

private[storage] case class SchemaInfo(schemaId: Int, schemaTypeName: String, schemaText: String, schemaHash: String)

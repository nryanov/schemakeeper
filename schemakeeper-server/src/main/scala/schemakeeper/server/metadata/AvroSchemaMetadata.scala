package schemakeeper.server.metadata

case class AvroSchemaMetadata(
                               subject: String,
                               schemaId: Int,
                               version: Int,
                               schema: String,
                             )


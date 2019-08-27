package schemakeeper.schema

/**
  * @param subject - subject name
  * @param id - global schema id
  * @param version - schema version specifically for current subject
  * @param schemaText - schema text
  */
final case class SchemaMetadata(subject: String, id: Int, version: Int, schemaText: String)

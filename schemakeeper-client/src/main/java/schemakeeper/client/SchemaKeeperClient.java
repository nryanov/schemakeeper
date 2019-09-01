package schemakeeper.client;

import org.apache.avro.Schema;

public interface SchemaKeeperClient {
    /**
     * @param id - schema id
     * @return - schema or null if schema with such id does not exist
     */
    Schema getSchemaById(int id);

    /**
     * @param subject - subject name
     * @return - list of schemas
     */
    Iterable<Schema> getSubjectSchemas(String subject);

    /**
     * @param schema - avro schema
     * @return - schema id or -1 if schema does not exist
     */
    int getSchemaId(Schema schema);

    /**
     * @param subject - subject name
     * @return - avro schema or null if schema does not exist
     */
    Schema getLastSubjectSchema(String subject);

    /**
     * @param subject - subject name
     * @param schema - new avro schema
     * @return - schema id
     */
    int registerNewSchema(String subject, Schema schema);

    void close();
}
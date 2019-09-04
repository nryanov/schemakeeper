package schemakeeper.client;

import org.apache.avro.Schema;
import schemakeeper.serialization.SerDeConfig;

public abstract class SchemaKeeperClient {
    protected SerDeConfig config;

    public SchemaKeeperClient(SerDeConfig config) {
        this.config = config;
    }

    /**
     * @param id - schema id
     * @return - schema or null if schema with such id does not exist
     */
    public abstract Schema getSchemaById(int id);

    /**
     * @param subject - subject name
     * @return - list of schemas
     */
    public abstract Iterable<Schema> getSubjectSchemas(String subject);

    /**
     * @param schema - avro schema
     * @return - schema id or -1 if schema does not exist
     */
    public abstract int getSchemaId(Schema schema);

    /**
     * @param subject - subject name
     * @return - avro schema or null if schema does not exist
     */
    public abstract Schema getLastSubjectSchema(String subject);

    /**
     * @param subject - subject name
     * @param schema - new avro schema
     * @return - schema id
     */
    public abstract int registerNewSchema(String subject, Schema schema);

    public void close() {

    }
}
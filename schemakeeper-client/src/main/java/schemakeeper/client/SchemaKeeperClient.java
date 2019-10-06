package schemakeeper.client;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

public abstract class SchemaKeeperClient {
    protected final static Logger logger = LoggerFactory.getLogger(SchemaKeeperClient.class);

    protected final String SCHEMAKEEPER_URL;

    protected SerDeConfig config;

    public SchemaKeeperClient(SerDeConfig config) {
        this.config = config;
        this.SCHEMAKEEPER_URL = this.config.schemakeeperUrlConfig();
    }

    /**
     * @param id - schema id
     * @return - schema
     */
    public abstract Schema getSchemaById(int id);

    /**
     * This method may register new schema and subject and connect them (if needed) and return schema id
     *
     * @param subject           - subject name
     * @param schema            - schema
     * @param schemaType        - type of schema
     * @param compatibilityType - compatibility type of subject which will be used if subject does not exist
     * @return - schema id
     */
    public abstract int registerNewSchema(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType);

    /**
     * This method only return schema id if subject and schema are both registered and connected
     *
     * @param subject    - subject name
     * @param schema     - schema text
     * @param schemaType - type of schema
     * @return - schema id
     */
    public abstract int getSchemaId(String subject, Schema schema, SchemaType schemaType);

    /**
     * Close opened resources
     */
    public abstract void close();
}
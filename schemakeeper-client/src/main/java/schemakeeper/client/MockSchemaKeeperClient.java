package schemakeeper.client;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.schema.AvroSchemaCompatibility;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Useful for debug or testing purposes
 */
public class MockSchemaKeeperClient extends SchemaKeeperClient {
    private static final Logger logger = LoggerFactory.getLogger(MockSchemaKeeperClient.class);

    private AvroSchemaCompatibility avroSchemaCompatibility;

    private int id;
    private Map<Schema, Integer> schemaId;
    private Map<Integer, Schema> idSchema;
    private Map<String, Map<Integer, Schema>> subjectSchemas;

    public MockSchemaKeeperClient(SerDeConfig config) {
        this(config, CompatibilityType.NONE);
    }

    public MockSchemaKeeperClient(SerDeConfig config, CompatibilityType compatibilityType) {
        super(config);

        this.id = 0;
        this.idSchema = new HashMap<>();
        this.schemaId = new HashMap<>();
        this.subjectSchemas = new HashMap<>();

        switch (compatibilityType) {
            case NONE:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.NONE_VALIDATOR;
                break;
            case BACKWARD:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.BACKWARD_VALIDATOR;
                break;
            case FORWARD:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.FORWARD_VALIDATOR;
                break;
            case FULL:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.FULL_VALIDATOR;
                break;
            case BACKWARD_TRANSITIVE:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR;
                break;
            case FORWARD_TRANSITIVE:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR;
                break;
            case FULL_TRANSITIVE:
                this.avroSchemaCompatibility = AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR;
                break;
        }
    }

    public AvroSchemaCompatibility getAvroSchemaCompatibility() {
        return avroSchemaCompatibility;
    }

    public int getId() {
        return id;
    }

    public Map<Schema, Integer> getSchemaId() {
        return Collections.unmodifiableMap(schemaId);
    }

    public Map<Integer, Schema> getIdSchema() {
        return Collections.unmodifiableMap(idSchema);
    }

    public Map<String, Map<Integer, Schema>> getSubjectSchemas() {
        return Collections.unmodifiableMap(subjectSchemas);
    }

    @Override
    public synchronized Schema getSchemaById(int id) {
        logger.debug("Get schema by id: {}", id);
        return idSchema.get(id);
    }

    @Override
    public synchronized Iterable<Schema> getSubjectSchemas(String subject) {
        logger.debug("Return schema by subject name: {}", subject);
        return subjectSchemas.getOrDefault(subject, Collections.emptyMap()).values();
    }

    @Override
    public synchronized int getSchemaId(Schema schema) {
        logger.debug("Get schema id for schema: {}", schema);
        return schemaId.getOrDefault(schema, -1);
    }

    @Override
    public synchronized Schema getLastSubjectSchema(String subject) {
        logger.debug("Get last schema for subject: {}", subject);

        Map<Integer, Schema> schemas = subjectSchemas.get(subject);

        if (schemas == null) {
            return null;
        } else {
            return schemas.values().stream().skip(schemas.size()).findFirst().orElseGet(() -> null);
        }
    }

    @Override
    public synchronized int registerNewSchema(String subject, Schema schema) {
        logger.debug("Register new schema for subject: {}, {}", subject, schema.toString());

        logger.debug("New schema: {}", schema);
        logger.debug("Old schema: {}", getLastSubjectSchema(subject));
        if (avroSchemaCompatibility.isCompatible(schema, getLastSubjectSchema(subject))) {
            id++;
            idSchema.put(id, schema);
            schemaId.put(schema, id);

            subjectSchemas.putIfAbsent(subject, new HashMap<>());
            subjectSchemas.get(subject).put(id, schema);
        } else {
            throw new RuntimeException("New schema is not compatible");
        }

        return id;
    }
}

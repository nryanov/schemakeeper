package schemakeeper.client;

import org.apache.avro.Schema;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachedSchemaKeeperClient extends DefaultSchemaKeeperClient {
    // used for write (Subject -> (Schema text -> schema id))
    private final ConcurrentHashMap<String, ConcurrentHashMap<Schema, Integer>> subjectSchemas = new ConcurrentHashMap<>();
    // used for read (Schema id -> schema)
    private final ConcurrentHashMap<Integer, Schema> idToSchema = new ConcurrentHashMap<>();

    public CachedSchemaKeeperClient(SerDeConfig config) {
        super(config);
    }

    @Override
    public Schema getSchemaById(int id) {
        return idToSchema.computeIfAbsent(id, integer -> getSchemaByIdRest(id));
    }

    public Schema getSchemaByIdRest(int id) {
        logger.info("Schema with id: {} not in cache. Trying to get it from server", id);
        return super.getSchemaById(id);
    }

    @Override
    public int registerNewSchema(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
        return subjectSchemas
                .computeIfAbsent(subject, s -> new ConcurrentHashMap<>())
                .computeIfAbsent(schema, schema1 -> registerNewSchemaRest(subject, schema1, schemaType, compatibilityType));
    }

    public int registerNewSchemaRest(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
        logger.info("Schema: {} for subject: {} not in cache. Trying to get it from server", subject, schema.toString());
        return super.registerNewSchema(subject, schema, schemaType, compatibilityType);
    }

    @Override
    public int getSchemaId(String subject, Schema schema, SchemaType schemaType) {
        return subjectSchemas
                .computeIfAbsent(subject, s -> new ConcurrentHashMap<>())
                .computeIfAbsent(schema, schema1 -> getSchemaIdRest(subject, schema1, schemaType));
    }

    public int getSchemaIdRest(String subject, Schema schema, SchemaType schemaType) {
        logger.info("Schema: {} for subject: {} not in cache. Trying to get it from server", subject, schema.toString());
        return super.getSchemaId(subject, schema, schemaType);
    }

    public Map<String, ConcurrentHashMap<Schema, Integer>> getSubjectSchemas() {
        return Collections.unmodifiableMap(subjectSchemas);
    }

    public Map<Integer, Schema> getIdToSchema() {
        return Collections.unmodifiableMap(idToSchema);
    }
}

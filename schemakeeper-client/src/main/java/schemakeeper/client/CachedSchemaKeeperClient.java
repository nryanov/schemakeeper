package schemakeeper.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.avro.Schema;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CachedSchemaKeeperClient extends DefaultSchemaKeeperClient {
    // used for write (Subject -> (Schema text -> schema id))
    private final Map<String, Map<Schema, Integer>> subjectSchemas;
    // used for read (Schema id -> schema)
    private final Map<Integer, Schema> idToSchema;

    public CachedSchemaKeeperClient(SerDeConfig config) {
        super(config);

        // todo: implement
        if (config.schemaCacheSize() != -1 || config.schemaCacheItemTtl() != -1) {
            CacheBuilder subjectSchemasBuilder = CacheBuilder.newBuilder();
            CacheBuilder idToSchemaBuilder = CacheBuilder.newBuilder();

            if (config.schemaCacheSize() > 0) {
                subjectSchemasBuilder.maximumSize(config.schemaCacheSize());
                idToSchemaBuilder.maximumSize(config.schemaCacheSize());
            }

            if (config.schemaCacheItemTtl() > 0) {
                subjectSchemasBuilder.expireAfterAccess(config.schemaCacheItemTtl(), TimeUnit.MILLISECONDS);
                idToSchemaBuilder.expireAfterAccess(config.schemaCacheItemTtl(), TimeUnit.MILLISECONDS);
            }

            subjectSchemas = new ConcurrentHashMap<>();
            idToSchema = new ConcurrentHashMap<>();
        } else {
            subjectSchemas = new ConcurrentHashMap<>();
            idToSchema = new ConcurrentHashMap<>();
        }
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

    public Map<String, Map<Schema, Integer>> getSubjectSchemas() {
        return Collections.unmodifiableMap(subjectSchemas);
    }

    public Map<Integer, Schema> getIdToSchema() {
        return Collections.unmodifiableMap(idToSchema);
    }
}

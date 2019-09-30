package schemakeeper.client;

import org.apache.avro.Schema;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Optional;
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
    public Optional<Schema> getSchemaById(int id) {
        Schema result = idToSchema.computeIfAbsent(id, integer -> {
            Optional<Schema> schema = CachedSchemaKeeperClient.super.getSchemaById(id);
            return schema.orElse(null);
        });

        return Optional.ofNullable(result);
    }

    @Override
    public Optional<Integer> registerNewSchema(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
        Integer result = subjectSchemas
                .computeIfAbsent(subject, s -> new ConcurrentHashMap<>())
                .computeIfAbsent(schema, schema1 -> {
                    Optional<Integer> id  = CachedSchemaKeeperClient.super.registerNewSchema(subject, schema1, schemaType, compatibilityType);
                    return id.orElse(null);
                });

        return Optional.ofNullable(result);
    }

    @Override
    public Optional<Integer> getSchemaId(String subject, Schema schema, SchemaType schemaType) {
        Integer result = subjectSchemas
                .computeIfAbsent(subject, s -> new ConcurrentHashMap<>())
                .computeIfAbsent(schema, schema1 -> {
                    Optional<Integer> id  = CachedSchemaKeeperClient.super.getSchemaId(subject, schema1, schemaType);
                    return id.orElse(null);
                });

        return Optional.ofNullable(result);
    }
}

package schemakeeper.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class InMemorySchemaKeeperClient implements AvroSchemaKeeperClient {
    private static final Logger logger = LoggerFactory.getLogger(InMemorySchemaKeeperClient.class);

    private int id = 0;
    private Map<Integer, Schema> ID_SCHEMA = new HashMap<>();
    private Map<Schema, Integer> SCHEMA_ID = new HashMap<>();
    private Map<String, Map<Integer, Schema>> SUBJECT_SCHEMAS = new HashMap<>();

    @Override
    public Schema getSchemaById(int id) {
        logger.debug("Get schema by id: {}", id);
        return ID_SCHEMA.get(id);
    }

    @Override
    public Iterable<Schema> getSubjectSchemas(String subject) {
        logger.debug("Return schema by subject name: {}", subject);
        return SUBJECT_SCHEMAS.get(subject).values();
    }

    @Override
    public Iterable<String> getSubjects() {
        logger.debug("Return all subjects");
        return SUBJECT_SCHEMAS.keySet();
    }

    @Override
    public int getSchemaId(String subject, Schema schema) {
        logger.debug("Get schema id for subject: {} and schema: {}", subject, schema.toString());
        return SCHEMA_ID.getOrDefault(schema, -1);
    }

    @Override
    public Schema getLastSubjectSchema(String subject) {
        logger.debug("Get last schema for subject: {}", subject);
        Map<Integer, Schema> schemas = SUBJECT_SCHEMAS.get(subject);

        if (schemas == null) {
            return null;
        } else {
            return schemas.values().stream().skip(schemas.values().size() - 2).findFirst().get();
        }
    }

    @Override
    public int registerNewSchema(String subject, Schema schema) {
        logger.debug("Register new schema for subject: {}, {}", subject, schema.toString());
        ++id;

        ID_SCHEMA.put(id, schema);
        SCHEMA_ID.put(schema, id);

        if (SUBJECT_SCHEMAS.get(subject) != null) {
            SUBJECT_SCHEMAS.get(subject).put(id, schema);
        } else {
            Map<Integer, Schema> schemas = new HashMap<>();
            schemas.put(id, schema);
            SUBJECT_SCHEMAS.put(subject, schemas);
        }

        return id;
    }

    @Override
    public boolean deleteSchemaById(int id) {
        logger.debug("Delete schema by id: {}", id);
        Schema schema = ID_SCHEMA.get(id);

        if (schema != null) {
            ID_SCHEMA.remove(id);
            SCHEMA_ID.remove(schema);
            SUBJECT_SCHEMAS.entrySet().forEach(entry -> {
                entry.getValue().remove(id);
            });

            return true;
        }
        return false;
    }
}

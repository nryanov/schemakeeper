package schemakeeper.serialization;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.Map;

public final class AvroSerDeConfig {
    public static final String SCHEMAKEEPER_URL_CONFIG = "schemakeeper_url";
    public static final String USE_SPECIFIC_READER_CONFIG = "use_specific_reader";
    public static final String ALLOW_FORCE_SCHEMA_REGISTER_CONFIG = "allow_force_schema_register";
    public static final String SPECIFIC_READER_SCHEMA_PER_SUBJECT_CONFIG = "specific_reader_schema_per_subject";

    private final Map<String, Object> config;

    public AvroSerDeConfig(Map<String, Object> config) {
        this.config = config;
    }

    public String schemakeeperUrlConfig() {
        if (!config.containsKey(SCHEMAKEEPER_URL_CONFIG)) {
            throw new IllegalArgumentException("schemakeeper_url is not specified");
        }

        return (String) config.get(SCHEMAKEEPER_URL_CONFIG);
    }

    public boolean useSpecificReader() {
        return (boolean) config.getOrDefault(USE_SPECIFIC_READER_CONFIG, false);
    }

    public boolean allowForceSchemaRegister() {
        return (boolean) config.getOrDefault(ALLOW_FORCE_SCHEMA_REGISTER_CONFIG, true);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Schema> specificReaderPerSubjectConfig() {
        return (Map<String, Schema>) config.getOrDefault(SPECIFIC_READER_SCHEMA_PER_SUBJECT_CONFIG, Collections.EMPTY_MAP);
    }
}

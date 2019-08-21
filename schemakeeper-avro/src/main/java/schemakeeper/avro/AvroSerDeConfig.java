package schemakeeper.avro;

import java.util.Map;

final public class AvroSerDeConfig {
    public static final String SCHEMAKEEPER_URL_CONFIG = "schemakeeper_url";
    public static final String USE_SPECIFIC_READER_CONFIG = "use_specific_reader";
    public static final String ALLOW_FORCE_SCHEMA_REGISTER_CONFIG = "allow_force_schema_register";
    public static final String SPECIFIC_READER_SCHEMA_PER_SUBJECT_CONFIG = "specific_reader_schema_per_subject";

    private final Map<String, Object> config;

    public AvroSerDeConfig(Map<String, Object> config) {
        this.config = config;
    }

    public int getInt(String key) {
        return (int) config.get(key);
    }

    public int getIntOrDefault(String key, int def) {
        return (int) config.getOrDefault(key, def);
    }

    public boolean getBoolean(String key) {
        return (boolean) config.get(key);
    }

    public boolean getBooleanOrDefault(String key, boolean def) {
        return (boolean) config.getOrDefault(key, def);
    }

    public String getString(String key) {
        return (String) config.get(key);
    }

    public String getStringOrDefault(String key, String def) {
        return (String) config.getOrDefault(key, def);
    }

    public Object get(String key) {
        return config.get(key);
    }

    public Object getOrDefault(String key, Object def) {
        return config.getOrDefault(key, def);
    }
}

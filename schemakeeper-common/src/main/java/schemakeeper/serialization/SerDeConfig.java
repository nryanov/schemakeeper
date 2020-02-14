package schemakeeper.serialization;

import schemakeeper.schema.CompatibilityType;

import java.util.Map;

public class SerDeConfig {
    public static final String SCHEMAKEEPER_URL_CONFIG = "schemakeeper.url";
    public static final String ALLOW_FORCE_SCHEMA_REGISTER_CONFIG = "allow.force.schema.register";
    public static final String COMPATIBILITY_TYPE = "compatibility.type";

    protected final Map<String, Object> config;

    public SerDeConfig(Map<String, Object> config) {
        this.config = config;
    }

    public String schemakeeperUrlConfig() {
        if (!config.containsKey(SCHEMAKEEPER_URL_CONFIG)) {
            throw new IllegalArgumentException("schemakeeper_url is not specified");
        }

        String url = (String) config.get(SCHEMAKEEPER_URL_CONFIG);

        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("schemakeeper_url is null or empty");
        }

        return url;
    }

    public boolean allowForceSchemaRegister() {
        return (boolean) config.getOrDefault(ALLOW_FORCE_SCHEMA_REGISTER_CONFIG, true);
    }

    public CompatibilityType compatibilityType() {
        return (CompatibilityType) config.getOrDefault(COMPATIBILITY_TYPE, CompatibilityType.BACKWARD);
    }
}

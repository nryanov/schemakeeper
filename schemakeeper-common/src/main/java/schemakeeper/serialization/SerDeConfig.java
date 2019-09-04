package schemakeeper.serialization;

import java.util.Map;

public class SerDeConfig {
    public static final String SCHEMAKEEPER_URL_CONFIG = "schemakeeper.url";
    public static final String ALLOW_FORCE_SCHEMA_REGISTER_CONFIG = "allow.force.schema.register";

    protected final Map<String, Object> config;

    public SerDeConfig(Map<String, Object> config) {
        this.config = config;
    }

    public String schemakeeperUrlConfig() {
        if (!config.containsKey(SCHEMAKEEPER_URL_CONFIG)) {
            throw new IllegalArgumentException("schemakeeper_url is not specified");
        }

        return (String) config.get(SCHEMAKEEPER_URL_CONFIG);
    }

    public boolean allowForceSchemaRegister() {
        return (boolean) config.getOrDefault(ALLOW_FORCE_SCHEMA_REGISTER_CONFIG, true);
    }
}

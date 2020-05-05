package schemakeeper.serialization;

import schemakeeper.configuration.Config;
import schemakeeper.exception.ConfigurationException;
import schemakeeper.schema.CompatibilityType;

import java.util.Map;

public class SerDeConfig extends Config {
    public static final String SCHEMAKEEPER_URL_CONFIG = "schemakeeper.url";
    public static final String ALLOW_FORCE_SCHEMA_REGISTER_CONFIG = "allow.force.schema.register";
    public static final String COMPATIBILITY_TYPE = "compatibility.type";

    // Max cache size. If -1, then size is unlimited. Value should be > 0
    public static final String SCHEMA_CACHE_SIZE = "schema.cache.size";
    // Cache item ttl in ms. If -1, then ttl is not used. Value should be > 0
    public static final String SCHEMA_CACHE_ITEM_TTL = "schema.cache.item.ttl";

    public SerDeConfig(Map<String, Object> config) {
        super(config);
    }

    public String schemakeeperUrlConfig() {
        if (!config.containsKey(SCHEMAKEEPER_URL_CONFIG)) {
            throw new ConfigurationException(String.format("%s is not specified", SCHEMAKEEPER_URL_CONFIG));
        }

        String url = (String) config.get(SCHEMAKEEPER_URL_CONFIG);

        if (url == null || url.isEmpty()) {
            throw new ConfigurationException(String.format("%s is null or empty", SCHEMAKEEPER_URL_CONFIG));
        }

        return url;
    }

    public boolean allowForceSchemaRegister() {
        return (boolean) config.getOrDefault(ALLOW_FORCE_SCHEMA_REGISTER_CONFIG, true);
    }

    public CompatibilityType compatibilityType() {
        return (CompatibilityType) config.getOrDefault(COMPATIBILITY_TYPE, CompatibilityType.BACKWARD);
    }

    public int schemaCacheSize() {
        int cacheSize = (int) config.getOrDefault(SCHEMA_CACHE_SIZE, -1);

        if (cacheSize != -1 && cacheSize <= 0) {
            throw new ConfigurationException(String.format("%s value should be positive or -1", SCHEMA_CACHE_SIZE));
        }

        return cacheSize;
    }

    public int schemaCacheItemTtl() {
        int cacheItemTtl = (int) config.getOrDefault(SCHEMA_CACHE_ITEM_TTL, -1);

        if (cacheItemTtl != -1 && cacheItemTtl <= 0) {
            throw new ConfigurationException(String.format("%s value should be positive or -1", SCHEMA_CACHE_ITEM_TTL));
        }

        return cacheItemTtl;
    }
}

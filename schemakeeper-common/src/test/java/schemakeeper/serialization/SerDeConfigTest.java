package schemakeeper.serialization;

import org.junit.Test;
import schemakeeper.schema.CompatibilityType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SerDeConfigTest {
    @Test
    public void getUrl() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "url");

        SerDeConfig serDeConfig = new SerDeConfig(cfg);

        assertEquals("url", serDeConfig.schemakeeperUrlConfig());
    }

    @Test
    public void throwErrorDueToNotSetUrlConfig() {
        SerDeConfig serDeConfig = new SerDeConfig(Collections.emptyMap());
        assertThrows(IllegalArgumentException.class, serDeConfig::schemakeeperUrlConfig);
    }

    @Test
    public void throwErrorDueToEmptyUrlConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "");

        SerDeConfig serDeConfig = new SerDeConfig(cfg);
        assertThrows(IllegalArgumentException.class, serDeConfig::schemakeeperUrlConfig);
    }

    @Test
    public void throwErrorDueToNullUrlConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, null);

        SerDeConfig serDeConfig = new SerDeConfig(cfg);
        assertThrows(IllegalArgumentException.class, serDeConfig::schemakeeperUrlConfig);
    }

    @Test
    public void getCompatibilityType() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.COMPATIBILITY_TYPE, CompatibilityType.FORWARD);

        SerDeConfig serDeConfig = new SerDeConfig(cfg);
        SerDeConfig defaultSerDeConfig = new SerDeConfig(Collections.emptyMap());

        assertEquals(CompatibilityType.FORWARD, serDeConfig.compatibilityType());
        assertEquals(CompatibilityType.BACKWARD, defaultSerDeConfig.compatibilityType());
    }

    @Test
    public void getForceSchemaRegistration() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.ALLOW_FORCE_SCHEMA_REGISTER_CONFIG, false);

        SerDeConfig serDeConfig = new SerDeConfig(cfg);
        SerDeConfig defaultSerDeConfig = new SerDeConfig(Collections.emptyMap());

        assertFalse(serDeConfig.allowForceSchemaRegister());
        assertTrue(defaultSerDeConfig.allowForceSchemaRegister());
    }
}

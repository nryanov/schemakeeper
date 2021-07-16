package schemakeeper.serialization;

import org.junit.Test;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.client.ClientConfig;
import schemakeeper.exception.ConfigurationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SerDeConfigTest {
    @Test
    public void throwErrorDueToNotSetUrlConfig() {
        SerDeConfig serDeConfig = new SerDeConfig(Collections.emptyMap());
        assertThrows(ConfigurationException.class, serDeConfig::schemakeeperUrlConfig);
    }

    @Test
    public void throwErrorDueToEmptyUrlConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "");

        SerDeConfig serDeConfig = new SerDeConfig(cfg);
        assertThrows(ConfigurationException.class, serDeConfig::schemakeeperUrlConfig);
    }

    @Test
    public void throwErrorDueToNullUrlConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, null);

        SerDeConfig serDeConfig = new SerDeConfig(cfg);
        assertThrows(ConfigurationException.class, serDeConfig::schemakeeperUrlConfig);
    }

    @Test
    public void shouldReturnUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "host:port");

        SerDeConfig config = new SerDeConfig(map);

        assertEquals("host:port", config.schemakeeperUrlConfig());
    }

    @Test
    public void shouldThrowErrorDueToEmptyUrl() {
        Map<String, Object> map = new HashMap<>();
        SerDeConfig config = new SerDeConfig(map);

        assertThrows(ConfigurationException.class, config::schemakeeperUrlConfig);
    }

    @Test
    public void shouldReturnCompatibilityType() {
        Map<String, Object> map = new HashMap<>();
        map.put(SerDeConfig.COMPATIBILITY_TYPE, CompatibilityType.FORWARD);

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(CompatibilityType.FORWARD, config.compatibilityType());
    }

    @Test
    public void shouldReturnDefaultCompatibilityType() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(CompatibilityType.BACKWARD, config.compatibilityType());
    }

    @Test
    public void shouldReturnForceSchemaRegistrationSetting() {
        Map<String, Object> map = new HashMap<>();
        map.put(SerDeConfig.ALLOW_FORCE_SCHEMA_REGISTER_CONFIG, false);

        SerDeConfig config = new SerDeConfig(map);

        assertFalse(config.allowForceSchemaRegister());
    }

    @Test
    public void shouldReturnDefaultForceSchemaRegistrationSetting() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertTrue(config.allowForceSchemaRegister());
    }

    @Test
    public void shouldReturnSocketTimeout() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_SOCKET_TIMEOUT, 1);

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(1, config.clientSocketTimeout());
    }

    @Test
    public void shouldReturnDefaultSocketTimeout() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(ClientConfig.DEFAULT_SOCKET_TIMEOUT, config.clientSocketTimeout());
    }

    @Test
    public void shouldReturnConnectTimeout() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_CONNECT_TIMEOUT, 1);

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(1, config.clientConnectTimeout());
    }

    @Test
    public void shouldReturnDefaultConnectTimeout() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(ClientConfig.DEFAULT_CONNECT_TIMEOUT, config.clientConnectTimeout());
    }

    @Test
    public void shouldReturnMaxConnections() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_MAX_CONNECTIONS, 1);

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(1, config.clientMaxConnections());
    }

    @Test
    public void shouldReturnDefaultMaxConnections() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(ClientConfig.DEFAULT_MAX_CONNECTIONS, config.clientMaxConnections());
    }

    @Test
    public void shouldReturnConnectionsPerRoute() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_CONNECTIONS_PER_ROUTE, 1);

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(1, config.clientConnectionsPerRoute());
    }

    @Test
    public void shouldReturnDefaultConnectionsPerRoute() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertEquals(ClientConfig.DEFAULT_CONNECTIONS_PER_ROUTE, config.clientConnectionsPerRoute());
    }

    @Test
    public void shouldReturnFalse() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertFalse(config.isProxied());
    }

    @Test
    public void shouldReturnFalseOnlyHost() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_PROXY_HOST, "host");

        SerDeConfig config = new SerDeConfig(map);

        assertFalse(config.isProxied());
    }

    @Test
    public void shouldReturnFalseOnlyPort() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_PROXY_PORT, 1);

        SerDeConfig config = new SerDeConfig(map);

        assertFalse(config.isProxied());
    }

    @Test
    public void shouldReturnProxyHostAndPort() {
        Map<String, Object> map = new HashMap<>();
        map.put(ClientConfig.CLIENT_PROXY_HOST, "host");
        map.put(ClientConfig.CLIENT_PROXY_PORT, 1);

        SerDeConfig config = new SerDeConfig(map);

        assertTrue(config.isProxied());
        assertEquals("host", config.clientProxyHost());
        assertEquals(1, config.clientProxyPort());
    }

    @Test
    public void shouldThrowsDueToEmptyProxySettings() {
        Map<String, Object> map = new HashMap<>();

        SerDeConfig config = new SerDeConfig(map);

        assertThrows(ConfigurationException.class, config::clientProxyHost);
        assertThrows(ConfigurationException.class, config::clientProxyPort);
    }
}

package schemakeeper.configuration;

import schemakeeper.client.ClientConfig;
import schemakeeper.exception.ConfigurationException;

import java.util.Map;
import java.util.function.Function;

public abstract class Config {
    protected final Map<String, Object> config;

    public Config(Map<String, Object> config) {
        this.config = config;
    }

    public int clientMaxConnections() {
        return (int) config.getOrDefault(ClientConfig.CLIENT_MAX_CONNECTIONS, ClientConfig.DEFAULT_MAX_CONNECTIONS);
    }

    public int clientConnectionsPerRoute() {
        return (int) config.getOrDefault(ClientConfig.CLIENT_CONNECTIONS_PER_ROUTE, ClientConfig.DEFAULT_CONNECTIONS_PER_ROUTE);
    }

    public int clientSocketTimeout() {
        return (int) config.getOrDefault(ClientConfig.CLIENT_SOCKET_TIMEOUT, ClientConfig.DEFAULT_SOCKET_TIMEOUT);
    }

    public int clientConnectTimeout() {
        return (int) config.getOrDefault(ClientConfig.CLIENT_CONNECT_TIMEOUT, ClientConfig.DEFAULT_CONNECT_TIMEOUT);
    }

    /**
     * @return true if http client should use proxy, otherwise - false.
     */
    public boolean isProxied() {
        return config.containsKey(ClientConfig.CLIENT_PROXY_HOST) && config.containsKey(ClientConfig.CLIENT_PROXY_PORT);
    }

    /**
     * Should be called only if isProxied() returned true
     */
    public String clientProxyHost() {
        if (!config.containsKey(ClientConfig.CLIENT_PROXY_HOST)) {
            throw new ConfigurationException(String.format("%s is not set but there was an attempt to get", ClientConfig.CLIENT_PROXY_HOST));
        }

        return (String) config.get(ClientConfig.CLIENT_PROXY_HOST);
    }

    /**
     * Should be called only if isProxied() returned true
     */
    public int clientProxyPort() {
        if (!config.containsKey(ClientConfig.CLIENT_PROXY_PORT)) {
            throw new ConfigurationException(String.format("%s is not set but there was an attempt to get", ClientConfig.CLIENT_PROXY_PORT));
        }

        return (int) config.get(ClientConfig.CLIENT_PROXY_PORT);
    }

    public String clientProxyUsername() {
        return (String) config.get(ClientConfig.CLIENT_PROXY_USERNAME);
    }

    public String clientProxyPassword() {
        return (String) config.get(ClientConfig.CLIENT_PROXY_PASSWORD);
    }
}

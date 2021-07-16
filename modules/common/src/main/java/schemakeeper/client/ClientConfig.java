package schemakeeper.client;

/**
 * Helper class with http client setting names and defaults.
 */
public abstract class ClientConfig {
    public static final String CLIENT_MAX_CONNECTIONS = "client.max.connections";
    public static final String CLIENT_CONNECTIONS_PER_ROUTE = "client.connections.per.route";
    public static final String CLIENT_SOCKET_TIMEOUT = "client.socket.timeout";
    public static final String CLIENT_CONNECT_TIMEOUT = "client.connect.timeout";

    public static final String CLIENT_PROXY_HOST = "client.proxy.host";
    public static final String CLIENT_PROXY_PORT = "client.proxy.port";

    // may be null
    public static final String CLIENT_PROXY_USERNAME = "client.proxy.username";
    // may be null
    public static final String CLIENT_PROXY_PASSWORD = "client.proxy.password";

    public static final int DEFAULT_MAX_CONNECTIONS = 30;
    public static final int DEFAULT_CONNECTIONS_PER_ROUTE = 5;
    public static final int DEFAULT_SOCKET_TIMEOUT = 60000;
    public static final int DEFAULT_CONNECT_TIMEOUT = 10000;
}

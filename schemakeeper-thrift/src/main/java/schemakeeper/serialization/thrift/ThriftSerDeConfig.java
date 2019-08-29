package schemakeeper.serialization.thrift;

import schemakeeper.serialization.SerDeConfig;

import java.util.Map;

public final class ThriftSerDeConfig extends SerDeConfig {
    public ThriftSerDeConfig(Map<String, Object> config) {
        super(config);
    }
}

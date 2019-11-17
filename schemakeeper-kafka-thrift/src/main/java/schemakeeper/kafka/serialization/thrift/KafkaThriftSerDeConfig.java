package schemakeeper.kafka.serialization.thrift;

import schemakeeper.kafka.naming.TopicNamingStrategy;
import schemakeeper.kafka.naming.NamingStrategy;
import schemakeeper.kafka.serialization.KafkaSerDeConfig;
import schemakeeper.serialization.thrift.ThriftSerDeConfig;

import java.util.Map;

public class KafkaThriftSerDeConfig extends ThriftSerDeConfig implements KafkaSerDeConfig {
    public KafkaThriftSerDeConfig(Map<String, Object> config) {
        super(config);
    }

    public NamingStrategy getKeyNamingStrategy() {
        return (NamingStrategy) config.getOrDefault(KEY_NAMING_STRATEGY_CONFIG, TopicNamingStrategy.INSTANCE);
    }

    public NamingStrategy getValueNamingStrategy() {
        return (NamingStrategy) config.getOrDefault(VALUE_NAMING_STRATEGY_CONFIG, TopicNamingStrategy.INSTANCE);
    }
}

package schemakeeper.kafka.serialization.protobuf;

import schemakeeper.kafka.naming.TopicNamingStrategy;
import schemakeeper.kafka.naming.NamingStrategy;
import schemakeeper.kafka.serialization.KafkaSerDeConfig;
import schemakeeper.serialization.protobuf.ProtobufSerDeConfig;

import java.util.Map;

public class KafkaProtobufSerDeConfig extends ProtobufSerDeConfig implements KafkaSerDeConfig {
    public KafkaProtobufSerDeConfig(Map<String, Object> config) {
        super(config);
    }

    public NamingStrategy getKeyNamingStrategy() {
        return (NamingStrategy) config.getOrDefault(KEY_NAMING_STRATEGY_CONFIG, TopicNamingStrategy.INSTANCE);
    }

    public NamingStrategy getValueNamingStrategy() {
        return (NamingStrategy) config.getOrDefault(VALUE_NAMING_STRATEGY_CONFIG, TopicNamingStrategy.INSTANCE);
    }
}
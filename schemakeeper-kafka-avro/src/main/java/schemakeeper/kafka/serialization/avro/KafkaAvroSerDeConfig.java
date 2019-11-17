package schemakeeper.kafka.serialization.avro;

import schemakeeper.kafka.naming.TopicNamingStrategy;
import schemakeeper.kafka.naming.NamingStrategy;
import schemakeeper.kafka.serialization.KafkaSerDeConfig;
import schemakeeper.serialization.avro.AvroSerDeConfig;

import java.util.Map;

public class KafkaAvroSerDeConfig extends AvroSerDeConfig implements KafkaSerDeConfig {
    public KafkaAvroSerDeConfig(Map<String, Object> config) {
        super(config);
    }

    public NamingStrategy getKeyNamingStrategy() {
        return (NamingStrategy) config.getOrDefault(KEY_NAMING_STRATEGY_CONFIG, TopicNamingStrategy.INSTANCE);
    }

    public NamingStrategy getValueNamingStrategy() {
        return (NamingStrategy) config.getOrDefault(VALUE_NAMING_STRATEGY_CONFIG, TopicNamingStrategy.INSTANCE);
    }
}

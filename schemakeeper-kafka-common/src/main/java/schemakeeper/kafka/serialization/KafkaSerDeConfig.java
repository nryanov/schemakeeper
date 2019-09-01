package schemakeeper.kafka.serialization;

import schemakeeper.kafka.NamingStrategy;

public interface KafkaSerDeConfig {
    String KEY_NAMING_STRATEGY_CONFIG = "key.naming.strategy";
    String VALUE_NAMING_STRATEGY_CONFIG = "value.naming.strategy";

    NamingStrategy getKeyNamingStrategy();

    NamingStrategy getValueNamingStrategy();
}

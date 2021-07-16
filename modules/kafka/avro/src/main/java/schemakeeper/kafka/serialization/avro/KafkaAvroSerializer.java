package schemakeeper.kafka.serialization.avro;

import org.apache.kafka.common.serialization.Serializer;
import schemakeeper.kafka.naming.TopicNamingStrategy;
import schemakeeper.kafka.naming.NamingStrategy;
import schemakeeper.schema.AvroSchemaUtils;
import schemakeeper.serialization.avro.AvroSerializer;

import java.util.Map;

public class KafkaAvroSerializer implements Serializer<Object> {
    private AvroSerializer serializer;
    private NamingStrategy namingStrategy;
    private boolean isKey;

    public KafkaAvroSerializer() {
    }

    public KafkaAvroSerializer(AvroSerializer serializer) {
        this.serializer = serializer;
        this.namingStrategy = TopicNamingStrategy.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KafkaAvroSerDeConfig config = new KafkaAvroSerDeConfig((Map<String, Object>) configs);
        this.isKey = isKey;
        this.namingStrategy = isKey ? config.getKeyNamingStrategy() : config.getValueNamingStrategy();
        this.serializer = new AvroSerializer(config);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return serializer.serialize(namingStrategy.resolveSubjectName(topic, isKey, AvroSchemaUtils.getSchema(data)), data);
    }

    @Override
    public void close() {
        serializer.close();
    }
}

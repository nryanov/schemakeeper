package schemakeeper.kafka.serialization.protobuf;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.common.serialization.Serializer;
import schemakeeper.kafka.DefaultNamingStrategy;
import schemakeeper.kafka.NamingStrategy;
import schemakeeper.serialization.protobuf.ProtobufSerializer;

import java.util.Map;

public class KafkaProtobufSerializer implements Serializer<com.google.protobuf.GeneratedMessageV3> {
    private ProtobufSerializer serializer;
    private NamingStrategy namingStrategy;
    private boolean isKey;

    public KafkaProtobufSerializer() {
    }

    public KafkaProtobufSerializer(ProtobufSerializer serializer) {
        this.serializer = serializer;
        this.namingStrategy = DefaultNamingStrategy.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KafkaProtobufSerDeConfig config = new KafkaProtobufSerDeConfig((Map<String, Object>) configs);
        this.isKey = isKey;
        this.namingStrategy = isKey ? config.getKeyNamingStrategy() : config.getValueNamingStrategy();
        this.serializer = new ProtobufSerializer(config);
    }

    @Override
    public byte[] serialize(String topic, GeneratedMessageV3 data) {
        return serializer.serialize(namingStrategy.resolveSubjectName(topic, isKey), data);
    }

    @Override
    public void close() {
        serializer.close();
    }
}

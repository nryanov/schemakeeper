package schemakeeper.kafka.serialization.protobuf;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.common.serialization.Deserializer;
import schemakeeper.serialization.protobuf.ProtobufDeserializer;

import java.util.Map;

public class KafkaProtobufDeserializer implements Deserializer<com.google.protobuf.GeneratedMessageV3> {
    private ProtobufDeserializer deserializer;
    boolean isKey;

    public KafkaProtobufDeserializer() {
    }

    public KafkaProtobufDeserializer(ProtobufDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KafkaProtobufSerDeConfig config = new KafkaProtobufSerDeConfig((Map<String, Object>) configs);
        this.isKey = isKey;
        this.deserializer = new ProtobufDeserializer(config);
    }

    @Override
    public GeneratedMessageV3 deserialize(String topic, byte[] data) {
        return deserializer.deserialize(data);
    }

    @Override
    public void close() {
        deserializer.close();
    }
}

package schemakeeper.kafka.serialization.protobuf;


import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaProtobufSerDe implements Serde<com.google.protobuf.GeneratedMessageV3> {
    private KafkaProtobufSerializer serializer;
    private KafkaProtobufDeserializer deserializer;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.serializer = new KafkaProtobufSerializer();
        this.deserializer = new KafkaProtobufDeserializer();

        this.serializer.configure(configs, isKey);
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<GeneratedMessageV3> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<GeneratedMessageV3> deserializer() {
        return deserializer;
    }
}

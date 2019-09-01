package schemakeeper.kafka.serialization.avro;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaAvroSerDe implements Serde<Object> {
    private KafkaAvroSerializer serializer;
    private KafkaAvroDeserializer deserializer;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.serializer = new KafkaAvroSerializer();
        this.deserializer = new KafkaAvroDeserializer();

        this.serializer.configure(configs, isKey);
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Object> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Object> deserializer() {
        return deserializer;
    }
}

package schemakeeper.kafka.serialization.avro;

import org.apache.kafka.common.serialization.Deserializer;
import schemakeeper.serialization.AvroDeserializer;

import java.util.Map;

public class KafkaAvroDeserializer implements Deserializer<Object> {
    private AvroDeserializer deserializer;
    private boolean isKey;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KafkaAvroSerDeConfig config = new KafkaAvroSerDeConfig((Map<String, Object>) configs);
        this.isKey = isKey;
        this.deserializer = new AvroDeserializer(config);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return deserializer.deserialize(data);
    }

    @Override
    public void close() {
        deserializer.close();
    }
}

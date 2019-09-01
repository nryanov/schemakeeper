package schemakeeper.kafka.serialization.thrift;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import schemakeeper.serialization.thrift.ThriftDeserializer;

import java.util.Map;

public class KafkaThriftDeserializer implements Deserializer<TBase<? extends TBase, ? extends TFieldIdEnum>> {
    private ThriftDeserializer deserializer;
    private boolean isKey;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KafkaThriftSerDeConfig config = new KafkaThriftSerDeConfig((Map<String, Object>) configs);
        this.isKey = isKey;
        this.deserializer = new ThriftDeserializer(config);
    }

    @Override
    public TBase<? extends TBase, ? extends TFieldIdEnum> deserialize(String topic, byte[] data) {
        return deserializer.deserialize(data);
    }

    @Override
    public void close() {
        deserializer.close();
    }
}

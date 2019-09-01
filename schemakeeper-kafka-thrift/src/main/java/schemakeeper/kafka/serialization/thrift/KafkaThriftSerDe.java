package schemakeeper.kafka.serialization.thrift;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import java.util.Map;

public class KafkaThriftSerDe implements Serde<TBase<? extends TBase, ? extends TFieldIdEnum>> {
    private KafkaThriftSerializer serializer;
    private KafkaThriftDeserializer deserializer;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.serializer = new KafkaThriftSerializer();
        this.deserializer = new KafkaThriftDeserializer();

        this.serializer.configure(configs, isKey);
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<TBase<? extends TBase, ? extends TFieldIdEnum>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TBase<? extends TBase, ? extends TFieldIdEnum>> deserializer() {
        return deserializer;
    }
}

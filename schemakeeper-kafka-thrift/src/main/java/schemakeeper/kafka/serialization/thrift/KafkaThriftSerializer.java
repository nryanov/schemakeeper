package schemakeeper.kafka.serialization.thrift;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import schemakeeper.kafka.DefaultNamingStrategy;
import schemakeeper.kafka.NamingStrategy;
import schemakeeper.serialization.thrift.ThriftSerializer;

import java.util.Map;

public class KafkaThriftSerializer implements Serializer<TBase<? extends TBase, ? extends TFieldIdEnum>> {
    private ThriftSerializer serializer;
    private NamingStrategy namingStrategy;
    private boolean isKey;

    public KafkaThriftSerializer() {
    }

    public KafkaThriftSerializer(ThriftSerializer serializer) {
        this.serializer = serializer;
        this.namingStrategy = DefaultNamingStrategy.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        KafkaThriftSerDeConfig config = new KafkaThriftSerDeConfig((Map<String, Object>) configs);
        this.isKey = isKey;
        this.namingStrategy = isKey ? config.getKeyNamingStrategy() : config.getValueNamingStrategy();
        this.serializer = new ThriftSerializer(config);
    }

    @Override
    public byte[] serialize(String topic, TBase<? extends TBase, ? extends TFieldIdEnum> data) {
        return serializer.serialize(namingStrategy.resolveSubjectName(topic, isKey), data);
    }

    @Override
    public void close() {
        serializer.close();
    }
}

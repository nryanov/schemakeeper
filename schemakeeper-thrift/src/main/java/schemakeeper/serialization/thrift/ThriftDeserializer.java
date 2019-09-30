package schemakeeper.serialization.thrift;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.thrift.ThriftDatumReader;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.client.CachedSchemaKeeperClient;
import schemakeeper.client.SchemaKeeperClient;
import schemakeeper.exception.DeserializationException;
import schemakeeper.exception.ThriftDeserializationException;
import schemakeeper.schema.thrift.SchemaKeeperThriftData;
import schemakeeper.serialization.AbstractDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Supplier;

public class ThriftDeserializer extends AbstractDeserializer<TBase<? extends TBase, ? extends TFieldIdEnum>> implements ThriftSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ThriftDeserializer.class);
    private final DecoderFactory decoderFactory;
    private final SchemaKeeperClient client;

    public ThriftDeserializer(SchemaKeeperClient client, ThriftSerDeConfig config) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
    }

    public ThriftDeserializer(SchemaKeeperClient client) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
    }

    public ThriftDeserializer(ThriftSerDeConfig config) {
        this.client = new CachedSchemaKeeperClient(config);
        this.decoderFactory = DecoderFactory.get();
    }

    public ThriftDeserializer(Map<String, Object> config) {
        this(new ThriftSerDeConfig(config));
    }

    @Override
    public TBase<? extends TBase, ? extends TFieldIdEnum> deserialize(byte[] data) throws ThriftDeserializationException {
        return deserialize(data, null);
    }

    public <T extends TBase<? extends TBase, ? extends TFieldIdEnum>> T deserialize(byte[] data, Class<T> clazz) throws ThriftDeserializationException {
        if (data == null) {
            return null;
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte protocolByte = readProtocolByte(buffer);
            if (protocolByte != THRIFT_BYTE) {
                throw new ThriftDeserializationException("This is not thrift-serialized data");
            }

            int schemaId = readSchemaId(buffer);
            Schema schema = client.getSchemaById(schemaId).orElseThrow((Supplier<ThriftDeserializationException>) () -> {
                throw new ThriftDeserializationException(String.format("Schema with id: %s does not exist", schemaId));
            });

            int dataLength = buffer.limit() - 5;
            int start = buffer.position() + buffer.arrayOffset();
            BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(buffer.array(), start, dataLength, null);
            ThriftDatumReader<T> reader = createReader(schema, clazz);
            T result = reader.read(null, binaryDecoder);

            return result;
        } catch (DeserializationException | IOException e) {
            throw new ThriftDeserializationException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private <T extends TBase<? extends TBase, ? extends TFieldIdEnum>> ThriftDatumReader<T> createReader(Schema schema, Class<T> clazz) {
        if (clazz == null) {
            return new ThriftDatumReader<>(schema);
        } else {
            return new ThriftDatumReader<>(schema, SchemaKeeperThriftData.get().getSchema(clazz));
        }
    }
}

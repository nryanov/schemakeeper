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

public class ThriftDeserializer<T extends TBase<? extends TBase, ? extends TFieldIdEnum>> extends AbstractDeserializer<T> implements ThriftSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ThriftDeserializer.class);
    private final DecoderFactory decoderFactory;
    private final SchemaKeeperClient client;
    private final Class<T> clazz;

    public ThriftDeserializer(SchemaKeeperClient client, ThriftSerDeConfig config) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
        this.clazz = null;
    }

    public ThriftDeserializer(SchemaKeeperClient client) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
        this.clazz = null;
    }

    public ThriftDeserializer(ThriftSerDeConfig config) {
        this.client = CachedSchemaKeeperClient.apply(config.schemakeeperUrlConfig());
        this.decoderFactory = DecoderFactory.get();
        this.clazz = null;
    }

    public ThriftDeserializer(Map<String, Object> config) {
        this(null, new ThriftSerDeConfig(config));
    }

    public ThriftDeserializer(SchemaKeeperClient client, ThriftSerDeConfig config, Class<T> clazz) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
        this.clazz = clazz;
    }

    public ThriftDeserializer(ThriftSerDeConfig config, Class<T> clazz) {
        this.client = CachedSchemaKeeperClient.apply(config.schemakeeperUrlConfig());
        this.decoderFactory = DecoderFactory.get();
        this.clazz = clazz;
    }

    public ThriftDeserializer(SchemaKeeperClient client, Class<T> clazz) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
        this.clazz = clazz;
    }

    public ThriftDeserializer(Map<String, Object> config, Class<T> clazz) {
        this(null, new ThriftSerDeConfig(config), clazz);
    }

    @Override
    public T deserialize(byte[] data) throws ThriftDeserializationException {
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
            Schema schema = client.getSchemaById(schemaId);

            if (schema == null) {
                throw new ThriftDeserializationException(String.format("Schema with id: %s does not exist", schemaId));
            }

            int dataLength = buffer.limit() - 5;
            int start = buffer.position() + buffer.arrayOffset();
            BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(buffer.array(), start, dataLength, null);
            ThriftDatumReader<T> reader = createReader(schema);
            T result = reader.read(null, binaryDecoder);

            return result;
        } catch (DeserializationException | IOException e) {
            throw new ThriftDeserializationException(e);
        }
    }

    private ThriftDatumReader<T> createReader(Schema schema) {
        if (clazz == null) {
            return new ThriftDatumReader<>(schema);
        } else {
            return new ThriftDatumReader<>(schema, SchemaKeeperThriftData.get().getSchema(clazz));
        }
    }
}

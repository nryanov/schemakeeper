package schemakeeper.serialization.protobuf;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.protobuf.ProtobufDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.client.CachedSchemaKeeperClient;
import schemakeeper.client.SchemaKeeperClient;
import schemakeeper.exception.ProtobufDeserializationException;
import schemakeeper.serialization.AbstractDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class ProtobufDeserializer extends AbstractDeserializer<Object> implements ProtobufSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufDeserializer.class);
    private final DecoderFactory decoderFactory;
    private final SchemaKeeperClient client;


    public ProtobufDeserializer(ProtobufSerDeConfig config) {
        this.client = CachedSchemaKeeperClient.apply(config.schemakeeperUrlConfig());
        this.decoderFactory = DecoderFactory.get();
    }

    public ProtobufDeserializer(SchemaKeeperClient client) {
        this.decoderFactory = DecoderFactory.get();
        this.client = client;
    }

    public ProtobufDeserializer(Map<String, Object> config) {
        this(new ProtobufSerDeConfig(config));
    }

    @Override
    public Object deserialize(byte[] data) throws ProtobufDeserializationException {
        return deserialize(data, null);
    }

    public <T extends com.google.protobuf.GeneratedMessageV3> T deserialize(byte[] data, Class<T> clazz) throws ProtobufDeserializationException {
        if (data == null) {
            return null;
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            byte protocol = readProtocolByte(buffer);

            if (protocol != PROTOBUF_BYTE) {
                throw new ProtobufDeserializationException("This is not protobuf-serialized data");
            }

            int schemaId = readSchemaId(buffer);
            Schema schema = client.getSchemaById(schemaId);

            if (schema == null) {
                throw new ProtobufDeserializationException(String.format("Schema with id: %s does not exist", schemaId));
            }

            int dataLength = buffer.limit() - 5;
            int start = buffer.position() + buffer.arrayOffset();
            BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(buffer.array(), start, dataLength, null);
            ProtobufDatumReader<T> reader = createReader(schema, clazz);
            T result = reader.read(null, binaryDecoder);

            return result;
        } catch (IOException e) {
            throw new ProtobufDeserializationException(e);
        }
    }

    private <T extends com.google.protobuf.GeneratedMessageV3> ProtobufDatumReader<T> createReader(Schema schema, Class<T> clazz) {
        if (clazz == null) {
            return new ProtobufDatumReader<>(schema);
        } else {
            return new ProtobufDatumReader<>(schema, ProtobufData.get().getSchema(clazz));
        }
    }
}

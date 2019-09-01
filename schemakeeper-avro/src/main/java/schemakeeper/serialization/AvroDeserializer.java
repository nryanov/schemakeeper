package schemakeeper.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.client.CachedSchemaKeeperClient;
import schemakeeper.client.SchemaKeeperClient;
import schemakeeper.exception.AvroDeserializationException;
import schemakeeper.schema.AvroSchemaUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class AvroDeserializer extends AbstractDeserializer<Object> implements AvroSerDe {
    private static final Logger logger = LoggerFactory.getLogger(AvroDeserializer.class);
    private final DecoderFactory decoderFactory;
    private final SchemaKeeperClient client;

    private boolean useSpecificReaderSchema;
    private final Map<String, Schema> readerSchemaCache;

    public AvroDeserializer(SchemaKeeperClient client, AvroSerDeConfig config) {
        this.client = client;
        this.decoderFactory = DecoderFactory.get();
        this.readerSchemaCache = new ConcurrentHashMap<>();
        this.useSpecificReaderSchema = config.useSpecificReader();

        readerSchemaCache.putAll(config.specificReaderPerSubjectConfig());
    }

    public AvroDeserializer(AvroSerDeConfig config) {
        this.client = CachedSchemaKeeperClient.apply(config.schemakeeperUrlConfig());
        this.decoderFactory = DecoderFactory.get();
        this.readerSchemaCache = new ConcurrentHashMap<>();
        this.useSpecificReaderSchema = config.useSpecificReader();

        readerSchemaCache.putAll(config.specificReaderPerSubjectConfig());
    }

    public AvroDeserializer(Map<String, Object> config) {
        this(new AvroSerDeConfig(config));
    }

    public Object deserialize(byte[] data) throws AvroDeserializationException {
        if (data == null) {
            return null;
        }

        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            readProtocolByte(byteBuffer); // todo: check protocol byte

            int id = byteBuffer.getInt();
            Schema schema = client.getSchemaById(id);

            int dataLength = byteBuffer.limit() - 5;

            if (schema.getType() == Schema.Type.BYTES) {
                return handleByteArray(byteBuffer, dataLength);
            }

            int start = byteBuffer.position() + byteBuffer.arrayOffset();

            BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(byteBuffer.array(), start, dataLength, null);
            DatumReader<Object> reader = createDatumReader(schema, null);
            Object result = reader.read(null, binaryDecoder);

            if (schema.getType() == Schema.Type.STRING) {
                return handleString(result);
            }

            return result;
        } catch (AvroDeserializationException e) {
            throw e;
        } catch (IOException e) {
            throw new AvroDeserializationException(e);
        }
    }

    public Optional<Object> deserializeSafe(byte[] data) {
        try {
            return Optional.of(deserialize(data));
        } catch (AvroDeserializationException e) {
            logger.warn("Deserialization error", e);
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private Object handleByteArray(ByteBuffer byteBuffer, int dataLength) {
        byte[] result = new byte[dataLength];
        byteBuffer.get(result);
        return result;
    }

    private Object handleString(Object result) {
        return result.toString();
    }

    private DatumReader<Object> createDatumReader(Schema writerSchema, Schema readerSchema) throws AvroDeserializationException {
        if (AvroSchemaUtils.isPrimitive(writerSchema)) {
            return new GenericDatumReader<>(writerSchema);
        }

        if (useSpecificReaderSchema) {
            if (readerSchema == null) {
                readerSchema = getReaderSchema(writerSchema);
            }

            return new SpecificDatumReader<>(writerSchema, readerSchema);
        } else {
            return readerSchema == null ? new GenericDatumReader<>(writerSchema) : new GenericDatumReader<>(writerSchema, readerSchema);
        }
    }

    @SuppressWarnings("unchecked")
    private Schema getReaderSchema(Schema writerSchema) throws AvroDeserializationException {
        Schema readerSchema = readerSchemaCache.get(writerSchema.getFullName());

        if (readerSchema == null) {
            try {
                Class<SpecificRecord> clazz = SpecificData.get().getClass(writerSchema);
                if (clazz != null) {
                    readerSchema = clazz.newInstance().getSchema();
                    readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
                } else {
                    throw new AvroDeserializationException("Specific record schema cannot be used due to not existing class");
                }
            } catch (InstantiationException | IllegalAccessException e) {
                throw new AvroDeserializationException("Error while getting class by name", e);
            }
        }

        return readerSchema;
    }
}

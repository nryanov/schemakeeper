package schemakeeper.serialization.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.client.CachedSchemaKeeperClient;
import schemakeeper.client.SchemaKeeperClient;
import schemakeeper.exception.AvroSerializationException;
import schemakeeper.exception.SerializationException;
import schemakeeper.schema.AvroSchemaUtils;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.AbstractSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class AvroSerializer extends AbstractSerializer<Object> implements AvroSerDe {
    private static final Logger logger = LoggerFactory.getLogger(AvroSerializer.class);
    private final EncoderFactory encoderFactory;
    private final SchemaKeeperClient client;
    private final boolean allowForceSchemaRegister;
    private final CompatibilityType compatibilityType;

    public AvroSerializer(SchemaKeeperClient client) {
        this.client = client;
        this.allowForceSchemaRegister = true;
        this.encoderFactory = EncoderFactory.get();
        this.compatibilityType = CompatibilityType.BACKWARD;
    }

    public AvroSerializer(SchemaKeeperClient client, AvroSerDeConfig config) {
        this.client = client;
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.compatibilityType = config.compatibilityType();
    }

    public AvroSerializer(AvroSerDeConfig config) {
        this.client = new CachedSchemaKeeperClient(config);
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.compatibilityType = config.compatibilityType();
    }

    public AvroSerializer(Map<String, Object> config) {
        this(new AvroSerDeConfig(config));
    }

    public byte[] serialize(String subject, Object value) throws AvroSerializationException {
        if (value == null) {
            return null;
        }

        try {
            Schema schema = AvroSchemaUtils.getSchema(value);
            int id;

            if (allowForceSchemaRegister) {
                id = client.registerNewSchema(subject, schema, SchemaType.AVRO, compatibilityType);
            } else {
                id = client.getSchemaId(subject, schema, SchemaType.AVRO);
            }

            if (id <= 0) {
                throw new IllegalArgumentException(String.format("Schema %s was not registered in registry", schema.toString()));
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeProtocolByte(out, AVRO_BYTE);
            writeSchemaId(out, id);

            if (value instanceof byte[]) {
                out.write((byte[]) value);
            } else {
                handleGeneric(out, value, schema);
            }

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | SerializationException e) {
            throw new AvroSerializationException(e);
        }
    }

    public Optional<byte[]> serializeSafe(String subject, Object value) {
        try {
            return Optional.ofNullable(serialize(subject, value));
        } catch (AvroSerializationException e) {
            logger.warn("Serialization error", e);
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private void handleGeneric(ByteArrayOutputStream out, Object value, Schema schema) throws IOException {
        BinaryEncoder binaryEncoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<Object> writer = createWriter(value, schema);
        writer.write(value, binaryEncoder);
        binaryEncoder.flush();
    }

    private DatumWriter<Object> createWriter(Object value, Schema schema) {
        if (value instanceof SpecificRecord) {
            return new SpecificDatumWriter<>(schema);
        } else {
            return new GenericDatumWriter<>(schema);
        }
    }
}

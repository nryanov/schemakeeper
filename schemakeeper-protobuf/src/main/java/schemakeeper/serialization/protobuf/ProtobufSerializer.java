package schemakeeper.serialization.protobuf;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.protobuf.ProtobufDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.client.CachedSchemaKeeperClient;
import schemakeeper.client.SchemaKeeperClient;
import schemakeeper.exception.ProtobufSerializationException;
import schemakeeper.exception.SerializationException;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.AbstractSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ProtobufSerializer extends AbstractSerializer<com.google.protobuf.GeneratedMessageV3> implements ProtobufSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ProtobufSerializer.class);
    private final EncoderFactory encoderFactory;
    private final SchemaKeeperClient client;
    private final boolean allowForceSchemaRegister;
    private final CompatibilityType compatibilityType;

    public ProtobufSerializer(SchemaKeeperClient client) {
        this.client = client;
        this.allowForceSchemaRegister = true;
        this.encoderFactory = EncoderFactory.get();
        this.compatibilityType = CompatibilityType.BACKWARD;
    }

    public ProtobufSerializer(SchemaKeeperClient client, ProtobufSerDeConfig config) {
        this.client = client;
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.compatibilityType = config.compatibilityType();
    }

    public ProtobufSerializer(ProtobufSerDeConfig config) {
        this.client = new CachedSchemaKeeperClient(config);
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.compatibilityType = config.compatibilityType();
    }

    public ProtobufSerializer(Map<String, Object> config) {
        this(new ProtobufSerDeConfig(config));
    }

    @Override
    public byte[] serialize(String subject, com.google.protobuf.GeneratedMessageV3 data) throws ProtobufSerializationException {
        if (data == null) {
            return null;
        }

        try {
            Schema schema = ProtobufData.get().getSchema(data.getClass());
            int id;

            if (allowForceSchemaRegister) {
                id = client.registerNewSchema(subject, schema, SchemaType.PROTOBUF, compatibilityType);
            } else {
                id = client.getSchemaId(subject, schema, SchemaType.PROTOBUF);
            }

            if (id <= 0) {
                throw new IllegalArgumentException(String.format("Schema %s was not registered in registry", schema.toString()));
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeProtocolByte(out, PROTOBUF_BYTE);
            writeSchemaId(out, id);
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            ProtobufDatumWriter<com.google.protobuf.GeneratedMessageV3> writer = new ProtobufDatumWriter<>(schema);
            writer.write(data, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;

        } catch (IOException | SerializationException e) {
            throw new ProtobufSerializationException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}

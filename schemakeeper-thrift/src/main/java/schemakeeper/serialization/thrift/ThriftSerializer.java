package schemakeeper.serialization.thrift;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.thrift.ThriftDatumWriter;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemakeeper.client.CachedSchemaKeeperClient;
import schemakeeper.client.SchemaKeeperClient;
import schemakeeper.exception.SerializationException;
import schemakeeper.exception.ThriftSerializationException;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.schema.thrift.SchemaKeeperThriftData;
import schemakeeper.serialization.AbstractSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ThriftSerializer extends AbstractSerializer<TBase<? extends TBase, ? extends TFieldIdEnum>> implements ThriftSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ThriftSerializer.class);
    private final EncoderFactory encoderFactory;
    private final SchemaKeeperClient client;
    private final boolean allowForceSchemaRegister;
    private final CompatibilityType compatibilityType;

    public ThriftSerializer(SchemaKeeperClient client) {
        this.client = client;
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = true;
        this.compatibilityType = CompatibilityType.BACKWARD;
    }

    public ThriftSerializer(SchemaKeeperClient client, ThriftSerDeConfig config) {
        this.client = client;
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.compatibilityType = config.compatibilityType();
    }

    public ThriftSerializer(ThriftSerDeConfig config) {
        this.client = new CachedSchemaKeeperClient(config);
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.compatibilityType = config.compatibilityType();
    }

    public ThriftSerializer(Map<String, Object> config) {
        this(new ThriftSerDeConfig(config));
    }

    @Override
    public byte[] serialize(String subject, TBase<? extends TBase, ? extends TFieldIdEnum> data) throws ThriftSerializationException {
        if (data == null) {
            return null;
        }

        ByteArrayOutputStream out;

        try {
            Schema schema = SchemaKeeperThriftData.get().getSchema(data.getClass());
            Optional<Integer> optId = client.getSchemaId(subject, schema, SchemaType.THRIFT);
            int id = -1;

            if (!optId.isPresent()) {
                if (allowForceSchemaRegister) {
                    optId = client.registerNewSchema(subject, schema, SchemaType.THRIFT, compatibilityType);
                    if (optId.isPresent()) {
                        id = optId.get();
                    } else {
                        throw new IllegalArgumentException(String.format("Schema %s is not registered in registry and flag 'allowForceSchemaRegister' is false ", schema.toString()));
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Schema %s is not registered in registry and flag 'allowForceSchemaRegister' is false ", schema.toString()));
                }
            }

            out = new ByteArrayOutputStream();
            writeProtocolByte(out, THRIFT_BYTE);
            writeSchemaId(out, id);

            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            ThriftDatumWriter<TBase<? extends TBase, ? extends TFieldIdEnum>> writer = new ThriftDatumWriter<>(schema);
            writer.write(data, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | SerializationException e) {
            throw new ThriftSerializationException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}

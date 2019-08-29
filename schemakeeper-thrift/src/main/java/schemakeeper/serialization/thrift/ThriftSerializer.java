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
import schemakeeper.schema.thrift.SchemaKeeperThriftData;
import schemakeeper.serialization.AbstractSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

//todo: allow to use only NONE and FULL compatibility types
public class ThriftSerializer<T extends TBase<? extends TBase, ? extends TFieldIdEnum>> extends AbstractSerializer<T> implements ThriftSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ThriftSerializer.class);
    private final EncoderFactory encoderFactory;
    private final SchemaKeeperClient client;
    private final boolean allowForceSchemaRegister;
    private final Class<T> clazz;

    public ThriftSerializer(SchemaKeeperClient client, Class<T> clazz) {
        this.client = client;
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = true;
        this.clazz = clazz;
    }

    public ThriftSerializer(SchemaKeeperClient client, ThriftSerDeConfig config, Class<T> clazz) {
        this.client = client;
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.clazz = clazz;
    }

    public ThriftSerializer(ThriftSerDeConfig config, Class<T> clazz) {
        this.client = CachedSchemaKeeperClient.apply(config.schemakeeperUrlConfig());
        this.encoderFactory = EncoderFactory.get();
        this.allowForceSchemaRegister = config.allowForceSchemaRegister();
        this.clazz = clazz;
    }

    public ThriftSerializer(Map<String, Object> config, Class<T> clazz) {
        this(null, new ThriftSerDeConfig(config), clazz);
    }

    @Override
    public byte[] serialize(String subject, T data) throws ThriftSerializationException {
        if (data == null) {
            return null;
        }

        ByteArrayOutputStream out;

        try {
            Schema schema = SchemaKeeperThriftData.get().getSchema(clazz);
            int id = client.getSchemaId(schema);

            if (id == -1) {
                if (allowForceSchemaRegister) {
                    id = client.registerNewSchema(subject, schema);
                } else {
                    throw new IllegalArgumentException(String.format("Schema %s is not registered in registry and flag 'allowForceSchemaRegister' is false ", schema.toString()));
                }
            }

            out = new ByteArrayOutputStream();
            writeProtocolByte(out, THRIFT_BYTE);
            writeSchemaId(out, id);

            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            ThriftDatumWriter<T> writer = new ThriftDatumWriter<>(schema);
            writer.write(data, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | SerializationException e) {
            throw new ThriftSerializationException(e);
        }
    }
}

package schemakeeper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

public class AvroSerializer extends AbstractAvroSerDe {
    private final EncoderFactory encoderFactory;

    public AvroSerializer(AvroSchemaKeeperClient client, AvroSerDeConfig config) {
        super(client, config);
        this.encoderFactory = EncoderFactory.get();
    }

    public AvroSerializer(AvroSerDeConfig config) {
        this(null, config);
    }

    public byte[] serialize(String subject, Object value) throws IOException {
        if (value == null) {
            return null;
        }

        Schema schema = AvroSchemaUtils.getSchema(value);
        int id = client.getSchemaId(subject, schema);

        if (id == -1) {
            id = client.registerNewSchema(subject, schema);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeAvroProtocolByte(out);
        writeAvroSchemaId(out, id);

        if (value instanceof byte[]) {
            out.write((byte[]) value);
        } else {
            handleGeneric(out, value, schema);
        }

        byte[] bytes = out.toByteArray();
        out.close();
        return bytes;
    }

    public Optional<byte[]> serializeSafe(String subject, Object value) {
        try {
            return Optional.ofNullable(serialize(subject, value));
        } catch (IOException e) {
            //todo: log error
            return Optional.empty();
        }
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

package schemakeeper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class AvroDeserializer extends AbstractAvroSerDe {
    private final DecoderFactory decoderFactory;

    public AvroDeserializer(AvroSchemaKeeperClient client) {
        super(client);
        this.decoderFactory = DecoderFactory.get();
    }

    public Object deserialize(byte[] data) throws IOException {
        if (data == null) {
            return null;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        readAvroProtocolByte(byteBuffer);

        int id = byteBuffer.getInt();
        // todo: may be null
        Schema schema = client.getSchemaById(id);

        int dataLength = byteBuffer.limit() - 5;

        if (schema.getType() == Schema.Type.BYTES) {
            return handleByteArray(byteBuffer, dataLength);
        }

        int start = byteBuffer.position() + byteBuffer.arrayOffset();

        BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(byteBuffer.array(), start, dataLength, null);
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object result = reader.read(null, binaryDecoder);

        if (schema.getType() == Schema.Type.STRING) {
            return handleString(result);
        }

        return result;
    }

    public Optional<Object> deserializeSafe(byte[] data) {
        try {
            return Optional.of(deserialize(data));
        } catch (IOException e) {
            //todo: log error
            return Optional.empty();
        }
    }

    private Object handleByteArray(ByteBuffer byteBuffer, int dataLength) {
        byte[] result = new byte[dataLength];
        byteBuffer.get(result);
        return result;
    }

    private Object handleString(Object result) {
        return result.toString();
    }
}

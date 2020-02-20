package schemakeeper.serialization;

import org.junit.jupiter.api.Test;
import schemakeeper.exception.SerializationException;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractSerializerTest {
    private AbstractSerializer<Object> serializer = new AbstractSerializer<Object>() {
        @Override
        public byte[] serialize(String subject, Object data) throws SerializationException {
            return new byte[0];
        }

        @Override
        public void close() {

        }
    };

    @Test
    public void writeProtocolByte() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeProtocolByte(out, (byte) 1);

        assertArrayEquals(new byte[] {(byte) 1}, out.toByteArray());
    }

    @Test
    public void writeSchemaId() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.writeSchemaId(out, 123);

        assertArrayEquals(new byte[] {(byte) 0, (byte) 0, (byte) 0, (byte) 123}, out.toByteArray());
    }
}

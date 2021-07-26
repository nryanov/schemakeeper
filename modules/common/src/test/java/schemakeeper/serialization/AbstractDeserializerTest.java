package schemakeeper.serialization;

import org.junit.Test;
import schemakeeper.exception.DeserializationException;
import schemakeeper.exception.SerializationException;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class AbstractDeserializerTest {
    AbstractDeserializer<Object> deserializer = new AbstractDeserializer<Object>() {
        @Override
        public Object deserialize(byte[] data) throws DeserializationException {
            return null;
        }

        @Override
        public void close() {

        }
    };

    @Test
    public void checkByteTest() {
        assertThrows(SerializationException.class, () -> deserializer.checkByte((byte) 1));
        deserializer.checkByte((byte) 0b1111001);
        deserializer.checkByte((byte) 0b1111011);
        deserializer.checkByte((byte) 0b1111010);
    }

    @Test
    public void readSchemaId() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(123);
        byteBuffer.flip();

        assertEquals(123, deserializer.readSchemaId(byteBuffer));
    }

    @Test
    public void readSchemaIdThrowsDeserializationException() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(0);

        assertThrows(DeserializationException.class, () -> deserializer.readSchemaId(byteBuffer));
    }

    @Test
    public void readProtocolByte() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.put((byte) 1);
        byteBuffer.flip();

        assertEquals(1, deserializer.readProtocolByte(byteBuffer));
    }

    @Test
    public void readProtocolByteThrowsDeserializationException() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(0);
        assertThrows(DeserializationException.class, () -> deserializer.readProtocolByte(byteBuffer));
    }
}

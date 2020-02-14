package schemakeeper.serialization;

import org.junit.jupiter.api.Test;
import schemakeeper.exception.DeserializationException;
import schemakeeper.exception.SerializationException;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractDeserializerTest {
    @Test
    public void checkByteTest() {
        AbstractDeserializer<Object> deserializer = new AbstractDeserializer<Object>() {
            @Override
            public Object deserialize(byte[] data) throws DeserializationException {
                return null;
            }

            @Override
            public void close() {

            }
        };

        assertThrows(SerializationException.class, () -> deserializer.checkByte((byte) 1));
        assertDoesNotThrow(() -> deserializer.checkByte((byte) 0b1111001));
        assertDoesNotThrow(() -> deserializer.checkByte((byte) 0b1111011));
        assertDoesNotThrow(() -> deserializer.checkByte((byte) 0b1111010));
    }
}

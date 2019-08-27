package schemakeeper.serialization;

import schemakeeper.exception.DeserializationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class AbstractDeserializer<T> implements Deserializer<T> {
    public byte readProtocolByte(InputStream in) throws DeserializationException {
        try {
            return (byte) in.read();
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    public int readSchemaId(InputStream in) throws DeserializationException {
        try {
            byte[] buffer = new byte[4];
            in.read(buffer);
            return ByteBuffer.wrap(buffer).getInt();
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    public final byte readProtocolByte(ByteBuffer in) {
        return in.get();
    }

    public final int readSchemaId(ByteBuffer in) {
        return in.getInt();
    }
}

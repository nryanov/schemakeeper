package schemakeeper.serialization;

import schemakeeper.exception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class AbstractSerializer<T> implements Serializer<T> {
    public void writeProtocolByte(OutputStream out, byte b) throws SerializationException {
        try {
            out.write(b);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    public void writeSchemaId(OutputStream out, int id) throws SerializationException {
        try {
            out.write(ByteBuffer.allocate(4).putInt(id).array());
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}

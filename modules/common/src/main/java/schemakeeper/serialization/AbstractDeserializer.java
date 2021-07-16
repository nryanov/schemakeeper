package schemakeeper.serialization;

import schemakeeper.exception.DeserializationException;
import schemakeeper.exception.SerializationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public abstract class AbstractDeserializer<T> implements Deserializer<T> {
    private static final byte AVRO_COMPATIBLE_MASK = 0b1111;

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
        try{
            return in.get();
        } catch (BufferUnderflowException e) {
            throw new DeserializationException(e);
        }
    }

    public final int readSchemaId(ByteBuffer in) {
        try {
            return in.getInt();
        } catch (BufferUnderflowException e) {
            throw new DeserializationException(e);
        }
    }

    public final void checkByte(byte b) {
        if (((b >> 3) ^ AVRO_COMPATIBLE_MASK) != 0) {
            throw new SerializationException("Schema type byte is not avro compatible");
        }
    }
}

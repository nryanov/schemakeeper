package schemakeeper.serialization;

import schemakeeper.exception.DeserializationException;

import java.io.Serializable;

public interface Deserializer<T> extends Serializable {
    T deserialize(byte[] data) throws DeserializationException;

    void close();
}

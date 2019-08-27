package schemakeeper.serialization;

import schemakeeper.exception.SerializationException;

import java.io.Serializable;

public interface Serializer<T> extends Serializable {
    byte[] serialize(String subject, T data) throws SerializationException;
}

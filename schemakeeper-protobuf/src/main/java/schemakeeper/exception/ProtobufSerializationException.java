package schemakeeper.exception;

public class ProtobufSerializationException extends SerializationException {
    public ProtobufSerializationException() {
    }

    public ProtobufSerializationException(String message) {
        super(message);
    }

    public ProtobufSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtobufSerializationException(Throwable cause) {
        super(cause);
    }

    public ProtobufSerializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

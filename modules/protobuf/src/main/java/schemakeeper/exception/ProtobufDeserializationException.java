package schemakeeper.exception;

public class ProtobufDeserializationException extends DeserializationException {
    public ProtobufDeserializationException() {
    }

    public ProtobufDeserializationException(String message) {
        super(message);
    }

    public ProtobufDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtobufDeserializationException(Throwable cause) {
        super(cause);
    }

    public ProtobufDeserializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

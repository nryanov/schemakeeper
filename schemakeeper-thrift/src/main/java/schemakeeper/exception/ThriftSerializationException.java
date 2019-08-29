package schemakeeper.exception;

public class ThriftSerializationException extends AvroSerializationException {
    public ThriftSerializationException() {
    }

    public ThriftSerializationException(String message) {
        super(message);
    }

    public ThriftSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftSerializationException(Throwable cause) {
        super(cause);
    }

    public ThriftSerializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

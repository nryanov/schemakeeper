package schemakeeper.exception;

public class ThriftDeserializationException extends AvroDeserializationException {
    public ThriftDeserializationException() {
    }

    public ThriftDeserializationException(String message) {
        super(message);
    }

    public ThriftDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftDeserializationException(Throwable cause) {
        super(cause);
    }

    public ThriftDeserializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

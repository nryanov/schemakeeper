package schemakeeper.avro.exception;

public class AvroDeserializationException extends AvroException {
    public AvroDeserializationException() {
    }

    public AvroDeserializationException(String message) {
        super(message);
    }

    public AvroDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroDeserializationException(Throwable cause) {
        super(cause);
    }

    public AvroDeserializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

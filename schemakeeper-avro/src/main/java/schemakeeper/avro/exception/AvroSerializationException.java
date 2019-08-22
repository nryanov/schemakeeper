package schemakeeper.avro.exception;

public class AvroSerializationException extends AvroException {
    public AvroSerializationException() {
    }

    public AvroSerializationException(String message) {
        super(message);
    }

    public AvroSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroSerializationException(Throwable cause) {
        super(cause);
    }

    public AvroSerializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

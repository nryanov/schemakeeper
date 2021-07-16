package schemakeeper.exception;

public class AvroSpecificRecordDeserializationException extends AvroDeserializationException {
    public AvroSpecificRecordDeserializationException() {
    }

    public AvroSpecificRecordDeserializationException(String message) {
        super(message);
    }

    public AvroSpecificRecordDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroSpecificRecordDeserializationException(Throwable cause) {
        super(cause);
    }

    public AvroSpecificRecordDeserializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

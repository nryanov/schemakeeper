package schemakeeper.avro.exception;

public class AvroSpecificRecordException extends AvroException {
    public AvroSpecificRecordException() {
    }

    public AvroSpecificRecordException(String message) {
        super(message);
    }

    public AvroSpecificRecordException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroSpecificRecordException(Throwable cause) {
        super(cause);
    }

    public AvroSpecificRecordException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

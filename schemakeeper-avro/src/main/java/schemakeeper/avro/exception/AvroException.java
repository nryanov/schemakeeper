package schemakeeper.avro.exception;

public class AvroException extends Exception {
    public AvroException() {
    }

    public AvroException(String message) {
        super(message);
    }

    public AvroException(String message, Throwable cause) {
        super(message, cause);
    }

    public AvroException(Throwable cause) {
        super(cause);
    }

    public AvroException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

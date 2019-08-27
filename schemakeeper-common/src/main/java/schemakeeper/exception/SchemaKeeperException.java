package schemakeeper.exception;

public class SchemaKeeperException extends Exception {
    public SchemaKeeperException() {
    }

    public SchemaKeeperException(String message) {
        super(message);
    }

    public SchemaKeeperException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchemaKeeperException(Throwable cause) {
        super(cause);
    }

    public SchemaKeeperException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

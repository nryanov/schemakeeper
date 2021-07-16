package schemakeeper.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

public class AvroSchemaUtils {
    private static final Map<Schema.Type, Schema> PRIMITIVE_TYPES;

    static {
        PRIMITIVE_TYPES = new HashMap<>();

        PRIMITIVE_TYPES.put(Schema.Type.STRING, Schema.create(Schema.Type.STRING));
        PRIMITIVE_TYPES.put(Schema.Type.BYTES, Schema.create(Schema.Type.BYTES));
        PRIMITIVE_TYPES.put(Schema.Type.INT, Schema.create(Schema.Type.INT));
        PRIMITIVE_TYPES.put(Schema.Type.LONG, Schema.create(Schema.Type.LONG));
        PRIMITIVE_TYPES.put(Schema.Type.FLOAT, Schema.create(Schema.Type.FLOAT));
        PRIMITIVE_TYPES.put(Schema.Type.DOUBLE, Schema.create(Schema.Type.DOUBLE));
        PRIMITIVE_TYPES.put(Schema.Type.BOOLEAN, Schema.create(Schema.Type.BOOLEAN));
        PRIMITIVE_TYPES.put(Schema.Type.NULL, Schema.create(Schema.Type.NULL));
    }

    public static Schema getSchema(Object value) {
        if (value == null) {
            return PRIMITIVE_TYPES.get(Schema.Type.NULL);
        } else if (value instanceof String) {
            return PRIMITIVE_TYPES.get(Schema.Type.STRING);
        } else if (value instanceof Integer) {
            return PRIMITIVE_TYPES.get(Schema.Type.INT);
        } else if (value instanceof Short) {
            return PRIMITIVE_TYPES.get(Schema.Type.INT);
        } else if (value instanceof Byte) {
            return PRIMITIVE_TYPES.get(Schema.Type.INT);
        } else if (value instanceof Long) {
            return PRIMITIVE_TYPES.get(Schema.Type.LONG);
        } else if (value instanceof Float) {
            return PRIMITIVE_TYPES.get(Schema.Type.FLOAT);
        } else if (value instanceof Double) {
            return PRIMITIVE_TYPES.get(Schema.Type.DOUBLE);
        } else if (value instanceof Boolean) {
            return PRIMITIVE_TYPES.get(Schema.Type.BOOLEAN);
        } else if (value instanceof byte[]) {
            return PRIMITIVE_TYPES.get(Schema.Type.BYTES);
        } else if (value instanceof GenericRecord) {
            return ((GenericRecord) value).getSchema();
        } else {
            throw new IllegalArgumentException("Unsupported avro type");
        }
    }

    public static boolean isPrimitive(Object value) {
        if (value == null) {
            return true;
        } else if (value instanceof String) {
            return true;
        } else if (value instanceof Integer) {
            return true;
        } else if (value instanceof Short) {
            return true;
        } else if (value instanceof Byte) {
            return true;
        } else if (value instanceof Long) {
            return true;
        } else if (value instanceof Float) {
            return true;
        } else if (value instanceof Double) {
            return true;
        } else if (value instanceof Boolean) {
            return true;
        } else if (value instanceof byte[]) {
            return true;
        } else if (value instanceof GenericRecord) {
            return false;
        } else {
            throw new IllegalArgumentException("Unsupported avro type");
        }
    }

    public static boolean isPrimitive(Schema schema) {
        return PRIMITIVE_TYPES.containsKey(schema.getType());
    }

    public static Schema parseSchema(String schema) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schema);
    }
}
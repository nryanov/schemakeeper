package schemakeeper.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AvroSchemaUtilsTest {
    @Test
    public void returnStringSchema() {
        String val = "";
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.STRING));
    }

    @Test
    public void returnBytesSchema() {
        byte[] val = new byte[0];
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.BYTES));
    }

    @Test
    public void returnIntSchema() {
        int val = 1;
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.INT));
    }

    @Test
    public void returnLongSchema() {
        long val = 1L;
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.LONG));
    }

    @Test
    public void returnFloatSchema() {
        float val = 1.0f;
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.FLOAT));
    }

    @Test
    public void returnDoubleSchema() {
        double val = 1.0d;
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.DOUBLE));
    }

    @Test
    public void returnBooleanSchema() {
        boolean val = true;
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.BOOLEAN));
    }

    @Test
    public void returnNullSchema() {
        Object val = null;
        assertEquals(AvroSchemaUtils.getSchema(val), Schema.create(Schema.Type.NULL));
    }

    @Test
    public void returnGenericSchema() {
        Schema schema = SchemaBuilder.builder()
                .record("test")
                .fields()
                .optionalString("f1")
                .endRecord();
        GenericRecord val = new GenericData.Record(schema);

        assertEquals(AvroSchemaUtils.getSchema(val), val.getSchema());
    }

    @Test
    public void checkIfStringIsPrimitive() {
        String val = "";
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfBytesIsPrimitive() {
        byte[] val = new byte[0];
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfIntIsPrimitive() {
        int val = 1;
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfLongIsPrimitive() {
        long val = 1L;
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfFloatIsPrimitive() {
        float val = 1.0f;
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfDoubleIsPrimitive() {
        double val = 1.0d;
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfBooleanIsPrimitive() {
        boolean val = true;
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfNullIsPrimitive() {
        Object val = null;
        assertTrue(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void checkIfGenericIsNotPrimitive() {
        Schema schema = SchemaBuilder.builder()
                .record("test")
                .fields()
                .optionalString("f1")
                .endRecord();
        GenericRecord val = new GenericData.Record(schema);

        assertFalse(AvroSchemaUtils.isPrimitive(val));
    }

    @Test
    public void throwErrorWhileGettingSchema() {
        String[] val = new String[0];
        assertThrows(IllegalArgumentException.class, () -> AvroSchemaUtils.getSchema(val));
    }

    @Test
    public void throwErrorWhileCheckingThatObjectIsPrimitive() {
        String[] val = new String[0];
        assertThrows(IllegalArgumentException.class, () -> AvroSchemaUtils.isPrimitive(val));
    }
}

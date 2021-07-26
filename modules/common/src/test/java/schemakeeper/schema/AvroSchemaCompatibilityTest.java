package schemakeeper.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import java.util.Arrays;
import static org.junit.Assert.*;


public class AvroSchemaCompatibilityTest {
    @Test
    public void nullPreviousSchemaShouldReturnTrueForAllCompatibilityTypes() {
        Schema a = Schema.create(Schema.Type.INT);

        assertTrue(AvroSchemaCompatibility.NONE_VALIDATOR.isCompatible(a, (Schema) null));
        assertTrue(AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(a, (Schema) null));
        assertTrue(AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(a, (Schema) null));
        assertTrue(AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(a, (Schema) null));
        assertTrue(AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(a, (Schema) null));
        assertTrue(AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(a, (Schema) null));
        assertTrue(AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(a, (Schema) null));
    }

    @Test
    public void noneCompatibilityValidatorAlwaysReturnTrue() {
        Schema a = Schema.create(Schema.Type.INT);
        Schema b = Schema.create(Schema.Type.STRING);
        Schema c = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .requiredString("f2")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.NONE_VALIDATOR.isCompatible(a, b));
        assertTrue(AvroSchemaCompatibility.NONE_VALIDATOR.isCompatible(b, c));
        assertTrue(AvroSchemaCompatibility.NONE_VALIDATOR.isCompatible(a, c));
    }

    @Test
    public void backwardCompatibilityReturnTrue() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        // add optional field
        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .optionalString("f3")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema));
    }

    @Test
    public void backwardCompatibilityReturnFalse() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        // add required field
        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .requiredString("f3")
                .endRecord();

        assertFalse(AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema));
    }

    @Test
    public void backwardCompatibilityReturnTrueWithNestedFields() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .optionalString("f3")
                .endRecord();

        Schema schemaWithNestedField = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .optionalString("f3")
                .name("nested")
                .type()
                .optional()
                .record("nested")
                .fields()
                .optionalString("nested_f1")
                .endRecord()
                .endRecord();

        assertTrue(AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema));
        assertTrue(AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(schemaWithNestedField, newSchema));
    }

    @Test
    public void forwardCompatibilityReturnTrue() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        // remove optional and add required and optional fields
        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .requiredString("f3")
                .optionalString("f4")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(newSchema, previousSchema));
    }

    @Test
    public void forwardCompatibilityReturnFalse() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        // delete required field
        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .optionalString("f3")
                .endRecord();

        assertFalse(AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(newSchema, previousSchema));
    }

    @Test
    public void fullCompatibilityReturnTrue() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        // remove optional and add fields
        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f3")
                .optionalString("f4")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(newSchema, previousSchema));
    }

    @Test
    public void fullCompatibilityReturnFalse() {
        Schema previousSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();

        // delete required field
        Schema newSchema = SchemaBuilder
                .builder()
                .record("test")
                .fields()
                .optionalString("f2")
                .optionalString("f3")
                .endRecord();

        assertFalse(AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(newSchema, previousSchema));
    }

    @Test
    public void backwardTransitiveCompatibilityReturnTrue() {
        Schema previousSchema1 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .endRecord();
        Schema previousSchema2 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();
        Schema newSchema = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .optionalString("f3")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, Arrays.asList(previousSchema1, previousSchema2)));
    }

    @Test
    public void backwardTransitiveCompatibilityReturnFalse() {
        Schema previousSchema1 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .endRecord();
        Schema previousSchema2 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();
        Schema newSchema = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .optionalString("f3")
                .requiredString("f4")
                .endRecord();

        assertFalse(AvroSchemaCompatibility.BACKWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, Arrays.asList(previousSchema1, previousSchema2)));
    }

    @Test
    public void forwardTransitiveCompatibilityReturnTrue() {
        Schema previousSchema1 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .endRecord();
        Schema previousSchema2 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();
        Schema newSchema = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .requiredString("f3")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, Arrays.asList(previousSchema1, previousSchema2)));
    }

    @Test
    public void forwardTransitiveCompatibilityReturnFalse() {
        Schema previousSchema1 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .endRecord();
        Schema previousSchema2 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();
        Schema newSchema = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f3")
                .endRecord();

        assertFalse(AvroSchemaCompatibility.FORWARD_TRANSITIVE_VALIDATOR.isCompatible(newSchema, Arrays.asList(previousSchema1, previousSchema2)));
    }

    @Test
    public void fullTransitiveCompatibilityReturnTrue() {
        Schema previousSchema1 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .endRecord();
        Schema previousSchema2 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();
        Schema newSchema = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .optionalString("f3")
                .endRecord();

        assertTrue(AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(newSchema, Arrays.asList(previousSchema1, previousSchema2)));
    }

    @Test
    public void fullTransitiveCompatibilityReturnFalse() {
        Schema previousSchema1 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .endRecord();
        Schema previousSchema2 = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .endRecord();
        Schema newSchema = SchemaBuilder
                .builder("namespace")
                .record("record")
                .fields()
                .requiredString("f1")
                .optionalString("f2")
                .requiredString("f3")
                .endRecord();

        assertFalse(AvroSchemaCompatibility.FULL_TRANSITIVE_VALIDATOR.isCompatible(newSchema, Arrays.asList(previousSchema1, previousSchema2)));
    }
}
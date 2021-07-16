package schemakeeper.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.Collections;
import java.util.List;

public class AvroSchemaCompatibility {
    public static final AvroSchemaCompatibility NONE_VALIDATOR = new AvroSchemaCompatibility((toValidate, existing) -> {
    });
    public static final AvroSchemaCompatibility BACKWARD_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canReadStrategy().validateLatest());
    public static final AvroSchemaCompatibility FORWARD_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canBeReadStrategy().validateLatest());
    public static final AvroSchemaCompatibility FULL_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().mutualReadStrategy().validateLatest());
    public static final AvroSchemaCompatibility BACKWARD_TRANSITIVE_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canReadStrategy().validateAll());
    public static final AvroSchemaCompatibility FORWARD_TRANSITIVE_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().canBeReadStrategy().validateAll());
    public static final AvroSchemaCompatibility FULL_TRANSITIVE_VALIDATOR = new AvroSchemaCompatibility(new SchemaValidatorBuilder().mutualReadStrategy().validateAll());

    private final SchemaValidator validator;

    public AvroSchemaCompatibility(SchemaValidator validator) {
        this.validator = validator;
    }

    public boolean isCompatible(Schema newSchema, Schema previousSchema) {
        if (previousSchema == null) {
            return true;
        }

        return isCompatible(newSchema, Collections.singletonList(previousSchema));
    }

    public boolean isCompatible(Schema newSchema, Iterable<Schema> previousSchemas) {
        try {
            validator.validate(newSchema, previousSchemas);
            return true;
        } catch (SchemaValidationException e) {
            return false;
        }
    }
}

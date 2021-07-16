package schemakeeper.api;

import org.apache.avro.Schema;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class SubjectAndSchemaRequest {
    private String schemaText;
    private SchemaType schemaType;
    private CompatibilityType compatibilityType;

    public static SubjectAndSchemaRequest instance(Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
        return new SubjectAndSchemaRequest(schema.toString(), schemaType, compatibilityType);
    }

    public static SubjectAndSchemaRequest instance(String schemaText, SchemaType schemaType, CompatibilityType compatibilityType) {
        return new SubjectAndSchemaRequest(schemaText, schemaType, compatibilityType);
    }

    public static SubjectAndSchemaRequest instance(String schemaText, SchemaType schemaType) {
        return new SubjectAndSchemaRequest(schemaText, schemaType, CompatibilityType.BACKWARD);
    }

    public SubjectAndSchemaRequest() {
    }

    public SubjectAndSchemaRequest(String schemaText, SchemaType schemaType, CompatibilityType compatibilityType) {
        this.schemaText = schemaText;
        this.schemaType = schemaType;
        this.compatibilityType = compatibilityType;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(SchemaType schemaType) {
        this.schemaType = schemaType;
    }

    public CompatibilityType getCompatibilityType() {
        return compatibilityType;
    }

    public void setCompatibilityType(CompatibilityType compatibilityType) {
        this.compatibilityType = compatibilityType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubjectAndSchemaRequest that = (SubjectAndSchemaRequest) o;
        return Objects.equals(schemaText, that.schemaText) &&
                schemaType == that.schemaType &&
                compatibilityType == that.compatibilityType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaText, schemaType, compatibilityType);
    }

    @Override
    public String toString() {
        return "SubjectAndSchemaRequest{" +
                "schemaText='" + schemaText + '\'' +
                ", schemaType=" + schemaType +
                ", compatibilityType=" + compatibilityType +
                '}';
    }
}

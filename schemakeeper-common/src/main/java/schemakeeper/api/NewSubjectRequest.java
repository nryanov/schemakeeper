package schemakeeper.api;

import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class NewSubjectRequest {
    private String schemaText;
    private SchemaType schemaType;
    private CompatibilityType compatibilityType;

    public static NewSubjectRequest instance(String schemaText, SchemaType schemaType, CompatibilityType compatibilityType) {
        return new NewSubjectRequest(schemaText, schemaType, compatibilityType);
    }

    public static NewSubjectRequest instance(String schemaText, SchemaType schemaType) {
        return new NewSubjectRequest(schemaText, schemaType, CompatibilityType.BACKWARD);
    }

    public NewSubjectRequest() {
    }

    public NewSubjectRequest(String schemaText, SchemaType schemaType, CompatibilityType compatibilityType) {
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
        NewSubjectRequest that = (NewSubjectRequest) o;
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
        return "NewSubjectRequest{" +
                "schemaText='" + schemaText + '\'' +
                ", schemaType=" + schemaType +
                ", compatibilityType=" + compatibilityType +
                '}';
    }
}

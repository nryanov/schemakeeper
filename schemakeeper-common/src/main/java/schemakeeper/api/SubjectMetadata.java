package schemakeeper.api;

import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class SubjectMetadata {
    private String subject;
    private CompatibilityType compatibilityType;
    private SchemaType schemaType;

    public static SubjectMetadata instance(String subject, CompatibilityType compatibilityType, SchemaType schemaType) {
        return new SubjectMetadata(subject, compatibilityType, schemaType);
    }

    public SubjectMetadata() {
    }

    public SubjectMetadata(String subject, CompatibilityType compatibilityType, SchemaType schemaType) {
        this.subject = subject;
        this.compatibilityType = compatibilityType;
        this.schemaType = schemaType;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public CompatibilityType getCompatibilityType() {
        return compatibilityType;
    }

    public void setCompatibilityType(CompatibilityType compatibilityType) {
        this.compatibilityType = compatibilityType;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(SchemaType schemaType) {
        this.schemaType = schemaType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubjectMetadata that = (SubjectMetadata) o;
        return Objects.equals(subject, that.subject) &&
                compatibilityType == that.compatibilityType &&
                schemaType == that.schemaType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, compatibilityType, schemaType);
    }

    @Override
    public String toString() {
        return "SubjectMetadata{" +
                "subject='" + subject + '\'' +
                ", compatibilityType=" + compatibilityType +
                ", schemaType=" + schemaType +
                '}';
    }
}

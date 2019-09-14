package schemakeeper.api;

import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;

import java.util.Arrays;
import java.util.Objects;

public class SubjectMetadata {
    private String subject;
    private CompatibilityType compatibilityType;
    private SchemaType schemaType;
    //todo: Remove versions
    private int[] versions;

    public static SubjectMetadata instance(String subject, CompatibilityType compatibilityType, SchemaType schemaType) {
        return new SubjectMetadata(subject, compatibilityType, schemaType);
    }

    public static SubjectMetadata instance(String subject, CompatibilityType compatibilityType, SchemaType schemaType, int[] versions) {
        return new SubjectMetadata(subject, compatibilityType, schemaType, versions);
    }

    public SubjectMetadata() {
    }

    public SubjectMetadata(String subject, CompatibilityType compatibilityType, SchemaType schemaType) {
        this(subject, compatibilityType, schemaType, new int[0]);
    }

    public SubjectMetadata(String subject, CompatibilityType compatibilityType, SchemaType schemaType, int[] versions) {
        this.subject = subject;
        this.compatibilityType = compatibilityType;
        this.schemaType = schemaType;
        this.versions = versions;
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

    public int[] getVersions() {
        return versions;
    }

    public void setVersions(int[] versions) {
        this.versions = versions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubjectMetadata that = (SubjectMetadata) o;
        return Objects.equals(subject, that.subject) &&
                compatibilityType == that.compatibilityType &&
                schemaType == that.schemaType &&
                Arrays.equals(versions, that.versions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(subject, compatibilityType, schemaType);
        result = 31 * result + Arrays.hashCode(versions);
        return result;
    }

    @Override
    public String toString() {
        return "SubjectMetadata{" +
                "subject='" + subject + '\'' +
                ", compatibilityType=" + compatibilityType +
                ", schemaType=" + schemaType +
                ", versions=" + Arrays.toString(versions) +
                '}';
    }
}

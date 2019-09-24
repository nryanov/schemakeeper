package schemakeeper.api;

import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class SubjectMetadata {
    private String subject;
    private CompatibilityType compatibilityType;

    public static SubjectMetadata instance(String subject, CompatibilityType compatibilityType) {
        return new SubjectMetadata(subject, compatibilityType);
    }

    public SubjectMetadata() {
    }

    public SubjectMetadata(String subject, CompatibilityType compatibilityType) {
        this.subject = subject;
        this.compatibilityType = compatibilityType;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubjectMetadata that = (SubjectMetadata) o;
        return Objects.equals(subject, that.subject) &&
                compatibilityType == that.compatibilityType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, compatibilityType);
    }

    @Override
    public String toString() {
        return "SubjectMetadata{" +
                "subject='" + subject + '\'' +
                ", compatibilityType=" + compatibilityType +
                '}';
    }
}

package schemakeeper.api;

import schemakeeper.schema.CompatibilityType;

import java.util.Objects;

public class SubjectMetadata {
    private String subject;
    private CompatibilityType compatibilityType;
    private String formatName;

    public static SubjectMetadata instance(String subject, CompatibilityType compatibilityType, String formatName) {
        return new SubjectMetadata(subject, compatibilityType, formatName);
    }

    public SubjectMetadata() {
    }

    public SubjectMetadata(String subject, CompatibilityType compatibilityType, String formatName) {
        this.subject = subject;
        this.compatibilityType = compatibilityType;
        this.formatName = formatName;
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

    public String getFormatName() {
        return formatName;
    }

    public void setFormatName(String formatName) {
        this.formatName = formatName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubjectMetadata that = (SubjectMetadata) o;
        return Objects.equals(subject, that.subject) &&
                compatibilityType == that.compatibilityType &&
                Objects.equals(formatName, that.formatName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, compatibilityType, formatName);
    }

    @Override
    public String toString() {
        return "SubjectMetadata{" +
                "subject='" + subject + '\'' +
                ", compatibilityType=" + compatibilityType +
                ", formatName='" + formatName + '\'' +
                '}';
    }
}

package schemakeeper.api;

import java.util.Objects;

public class SchemaMetadata {
    private String subject;
    private int id;
    private int version;
    private String schemaText;

    public static SchemaMetadata instance(String subject, int id, int version, String schemaText) {
        return new SchemaMetadata(subject, id, version, schemaText);
    }

    public SchemaMetadata() {
    }

    public SchemaMetadata(String subject, int id, int version, String schemaText) {
        this.subject = subject;
        this.id = id;
        this.version = version;
        this.schemaText = schemaText;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaMetadata that = (SchemaMetadata) o;
        return id == that.id &&
                version == that.version &&
                Objects.equals(subject, that.subject) &&
                Objects.equals(schemaText, that.schemaText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, id, version, schemaText);
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "subject='" + subject + '\'' +
                ", id=" + id +
                ", version=" + version +
                ", schemaText='" + schemaText + '\'' +
                '}';
    }
}

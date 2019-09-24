package schemakeeper.api;

import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class SubjectSchemaMetadata {
    private int schemaId;
    private int version;
    private String schemaText;
    private String schemaHash;
    private SchemaType schemaType;

    public static SubjectSchemaMetadata instance(int schemaId, int version, String schemaText, String schemaHash, SchemaType schemaType) {
        return new SubjectSchemaMetadata(schemaId, version, schemaText, schemaHash, schemaType);
    }

    public static SubjectSchemaMetadata instance(int schemaId, int version, String schemaText, String schemaHash) {
        return new SubjectSchemaMetadata(schemaId, version, schemaText, schemaHash, SchemaType.AVRO);
    }

    public SubjectSchemaMetadata() {
    }

    public SubjectSchemaMetadata(int schemaId, int version, String schemaText, String schemaHash, SchemaType schemaType) {
        this.schemaId = schemaId;
        this.version = version;
        this.schemaText = schemaText;
        this.schemaHash = schemaHash;
        this.schemaType = schemaType;
    }


    public int getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(int schemaId) {
        this.schemaId = schemaId;
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

    public String getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(String schemaHash) {
        this.schemaHash = schemaHash;
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
        SubjectSchemaMetadata that = (SubjectSchemaMetadata) o;
        return version == that.version &&
                schemaId == that.schemaId &&
                Objects.equals(schemaText, that.schemaText) &&
                Objects.equals(schemaHash, that.schemaHash) &&
                schemaType == that.schemaType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, schemaId, schemaText, schemaHash, schemaType);
    }

    @Override
    public String toString() {
        return "SubjectSchemaMetadata{" +
                "version=" + version +
                ", schemaId=" + schemaId +
                ", schemaText='" + schemaText + '\'' +
                ", schemaHash='" + schemaHash + '\'' +
                ", schemaType=" + schemaType +
                '}';
    }
}

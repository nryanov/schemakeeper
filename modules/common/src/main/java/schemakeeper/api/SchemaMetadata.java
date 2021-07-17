package schemakeeper.api;

import org.apache.avro.Schema;
import schemakeeper.schema.AvroSchemaUtils;
import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class SchemaMetadata {
    private int schemaId;
    private String schemaText;
    private String schemaHash;
    private SchemaType schemaType;

    public static SchemaMetadata instance(int schemaId, String schemaText, String schemaHash, SchemaType schemaType) {
        return new SchemaMetadata(schemaId, schemaText, schemaHash, schemaType);
    }

    public static SchemaMetadata instance(int schemaId, String schemaText, String schemaHash) {
        return new SchemaMetadata(schemaId, schemaText, schemaHash, SchemaType.AVRO);
    }

    public SchemaMetadata() {
    }

    public SchemaMetadata(int schemaId, String schemaText, String schemaHash) {
        this(schemaId, schemaText, schemaHash, SchemaType.AVRO);
    }

    public SchemaMetadata(int schemaId, String schemaText, String schemaHash, SchemaType schemaType) {
        this.schemaId = schemaId;
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

    public String getSchemaText() {
        return schemaText;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    public Schema getSchema() {
        return AvroSchemaUtils.parseSchema(schemaText);
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
        SchemaMetadata that = (SchemaMetadata) o;
        return schemaId == that.schemaId &&
                Objects.equals(schemaText, that.schemaText) &&
                Objects.equals(schemaHash, that.schemaHash) &&
                schemaType == that.schemaType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, schemaText, schemaHash, schemaType);
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "schemaId=" + schemaId +
                ", schemaText='" + schemaText + '\'' +
                ", schemaHash='" + schemaHash + '\'' +
                ", schemaType=" + schemaType +
                '}';
    }
}

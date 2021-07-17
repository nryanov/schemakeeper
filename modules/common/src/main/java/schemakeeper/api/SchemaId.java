package schemakeeper.api;

import java.util.Objects;

public class SchemaId {
    private int schemaId;

    public static SchemaId instance(int schemaId) {
        return new SchemaId(schemaId);
    }

    public SchemaId() {
    }

    public SchemaId(int schemaId) {
        this.schemaId = schemaId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(int schemaId) {
        this.schemaId = schemaId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaId schemaId1 = (SchemaId) o;
        return schemaId == schemaId1.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId);
    }

    @Override
    public String toString() {
        return "SchemaId{" +
                "schemaId=" + schemaId +
                '}';
    }
}

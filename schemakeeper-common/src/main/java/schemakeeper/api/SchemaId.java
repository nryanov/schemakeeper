package schemakeeper.api;

import java.util.Objects;

public class SchemaId {
    private int id;

    public static SchemaId instance(int id) {
        return new SchemaId(id);
    }

    public SchemaId() {
    }

    public SchemaId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaId schemaId = (SchemaId) o;
        return id == schemaId.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "SchemaId{" +
                "id=" + id +
                '}';
    }
}

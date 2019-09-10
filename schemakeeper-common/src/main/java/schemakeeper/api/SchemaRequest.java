package schemakeeper.api;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Objects;

public class SchemaRequest {
    private String schemaText;

    public static SchemaRequest instance(String schema) {
        return new SchemaRequest(schema);
    }

    public static SchemaRequest instance(Schema schema) {
        return new SchemaRequest(schema);
    }


    public SchemaRequest() {
    }

    public SchemaRequest(Schema schema) {
        this(schema.toString());
    }

    public SchemaRequest(String schemaText) {
        this.schemaText = schemaText;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    public Schema getSchema() {
        Schema.Parser parser = new Schema.Parser();

        return parser.parse(schemaText);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaRequest that = (SchemaRequest) o;
        return Objects.equals(schemaText, that.schemaText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaText);
    }

    @Override
    public String toString() {
        return "SchemaRequest{" +
                "schemaText='" + schemaText + '\'' +
                '}';
    }
}

package schemakeeper.api;

import org.apache.avro.Schema;

import java.util.Objects;

public class SchemaResponse {
    private String schemaText;

    public static SchemaResponse instance(String schema) {
        return new SchemaResponse(schema);
    }

    public static SchemaResponse instance(Schema schema) {
        return new SchemaResponse(schema);
    }


    public SchemaResponse() {
    }

    public SchemaResponse(Schema schema) {
        this(schema.toString());
    }

    public SchemaResponse(String schemaText) {
        this.schemaText = schemaText;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public Schema getSchema() {
        Schema.Parser parser = new Schema.Parser();

        return parser.parse(schemaText);
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaResponse that = (SchemaResponse) o;
        return Objects.equals(schemaText, that.schemaText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaText);
    }

    @Override
    public String toString() {
        return "SchemaResponse{" +
                "schemaText='" + schemaText + '\'' +
                '}';
    }
}

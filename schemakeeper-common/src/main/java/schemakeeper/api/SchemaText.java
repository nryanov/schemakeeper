package schemakeeper.api;

import org.apache.avro.Schema;
import schemakeeper.schema.SchemaType;

import java.util.Objects;

public class SchemaText {
    private String schemaText;
    private SchemaType schemaType;

    public static SchemaText instance(String schema) {
        return new SchemaText(schema, SchemaType.AVRO);
    }

    public static SchemaText instance(Schema schema) {
        return new SchemaText(schema);
    }

    public static SchemaText instance(String schema, SchemaType schemaType) {
        return new SchemaText(schema, schemaType);
    }

    public SchemaText() {
    }

    public SchemaText(Schema schema) {
        this(schema.toString(), SchemaType.AVRO);
    }

    public SchemaText(String schemaText, SchemaType schemaType) {
        this.schemaText = schemaText;
        this.schemaType = schemaType;
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
        SchemaText that = (SchemaText) o;
        return Objects.equals(schemaText, that.schemaText) &&
                schemaType == that.schemaType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaText, schemaType);
    }

    @Override
    public String toString() {
        return "SchemaText{" +
                "schemaText='" + schemaText + '\'' +
                ", schemaType=" + schemaType +
                '}';
    }
}
